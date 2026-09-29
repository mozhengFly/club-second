#!/usr/bin/env node
/**
 * download.js —— 从 GitHub Release 下载附件（默认 check-in.bundle.zip）到当前目录
 *
 * 用法:
 *   node download.js                    下载默认版本（v1.0.0）的默认附件到当前目录
 *   node download.js --tag v1.0.0       指定版本（默认取脚本内 DEFAULT_TAG）
 *   node download.js --all              下载该 Release 下的全部附件
 *   node download.js -a a.zip -a b.zip  指定多个附件（也可写成 -a a.zip,b.zip）
 *   node download.js --list             只列出该 Release 下的所有附件
 *   node download.js --out dist         换输出目录（默认当前目录）
 *   node download.js --force            已存在也强制重新下载
 *
 * 备份/恢复: 下载前若当前目录已有同名文件，会先备份到 backup/（文件夹再次运行会先清空）；
 *            下载失败（含 sha256 校验失败）时自动从 backup/ 恢复原文件。
 *            网络失败自动重试，最多 3 次。
 *
 * 依赖: 只需 Node 18+（用内置 fetch，无需 npm install）
 *
 * 仓库: 默认取脚本内写死的 DEFAULT_REPO，因此本脚本可以单独拷到任意空目录运行，
 *       不需要该目录是 git 仓库。取值顺序为
 *       --repo 参数 -> 环境变量 CLUB_REPO / GITHUB_REPOSITORY -> DEFAULT_REPO -> git remote
 * Token: 仓库是私有时才需要。取值顺序为
 *        --token 参数 -> 环境变量 GITHUB_TOKEN -> GH_TOKEN
 *        -> 当前目录 .github-token -> 脚本所在目录 .github-token
 * 输出: --out 的相对路径基于「运行时的当前目录」，绝对路径原样使用。
 */

'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { Readable, Transform } = require('stream');
const { pipeline } = require('stream/promises');
const { execFileSync } = require('child_process');

// ---------------------------------------------------------------- 常量配置

const API_BASE = 'https://api.github.com';
const API_VER = '2022-11-28';
const ROOT = __dirname; // download.js 所在目录
const CWD = process.cwd(); // 运行时目录，--out 的相对路径基于它

/**
 * 默认仓库 owner/repo。
 * 本脚本经常被单独拷到一个空目录里运行（那里没有 .git），所以这里写死默认值；
 * 需要换仓库时用 --repo owner/repo 或环境变量 CLUB_REPO 覆盖，不用改代码。
 * 如果留空字符串 ''，则退回到从 git remote origin 解析。
 */
const DEFAULT_REPO = 'mozhengFly/club';

/**
 * 默认要下载的附件列表（数组，可以写多个）。
 *   - 写多个文件名：全部下载到同一个输出目录
 *   - 写成空数组 []：等同于 --all，下载该 Release 下的全部附件
 * 例如： const DEFAULT_ASSETS = ['check-in.bundle.zip', 'crypto.bundle.js'];
 */
const DEFAULT_ASSETS = ['check-in.bundle.zip'];

/**
 * 默认要下载的 Release 版本号。
 * 改这里的 DEFAULT_TAG 即可切换要下载的版本，也可运行时用 --tag / -v 覆盖。
 */
const DEFAULT_TAG = 'v1.0.0';

const DEFAULT_OUT = '.'; // 默认下载到当前目录
const USER_AGENT = 'club-download.js';
const DOWNLOAD_ATTEMPTS = 3; // 单个文件最多尝试次数
const DOWNLOAD_TIMEOUT_MS = 10 * 60 * 1000; // 单个文件下载的兜底超时

// ---------------------------------------------------------------- 工具函数

function human(bytes) {
  if (!bytes && bytes !== 0) return '-';
  const units = ['B', 'KB', 'MB', 'GB'];
  let n = bytes;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${i === 0 ? n : n.toFixed(2)} ${units[i]}`;
}

function log(msg = '') {
  process.stdout.write(`${msg}\n`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// undici 的 fetch 抛错时原因藏在 err.cause 里，单独提出来便于排错
function describeFetchError(err) {
  const cause = err && err.cause;
  if (!cause) return err.message;
  const detail = cause.code || cause.message || String(cause);
  return `${err.message} (${detail})`;
}

// 把 "a.zip,b.zip" 拆成 ['a.zip','b.zip']
function splitNames(value) {
  return String(value)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseArgs(argv) {
  const opts = {
    tag: DEFAULT_TAG,
    assets: [...DEFAULT_ASSETS],
    all: DEFAULT_ASSETS.length === 0,
    out: DEFAULT_OUT,
    repo: '',
    token: '',
    force: false,
    list: false,
    help: false,
  };

  const takeValue = (name, inline, i) => {
    if (inline) return { value: inline, next: i };
    if (i + 1 >= argv.length) throw new Error(`选项 ${name} 缺少参数`);
    return { value: argv[i + 1], next: i + 1 };
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    let name = arg;
    let inline = '';
    const eq = arg.indexOf('=');
    if (arg.startsWith('--') && eq > -1) {
      name = arg.slice(0, eq);
      inline = arg.slice(eq + 1);
    }

    switch (name) {
      case '--tag':
      case '-v': {
        const r = takeValue(name, inline, i);
        opts.tag = r.value;
        i = r.next;
        break;
      }
      case '--asset':
      case '-a': {
        const r = takeValue(name, inline, i);
        const names = splitNames(r.value);
        if (!names.length) throw new Error('--asset 的附件名不能为空');
        // 第一次显式指定时，丢掉默认列表，避免"默认 + 指定"混在一起
        if (!opts._assetGiven) {
          opts.assets = [];
          opts._assetGiven = true;
        }
        opts.assets.push(...names);
        i = r.next;
        break;
      }
      case '--out':
      case '-o': {
        const r = takeValue(name, inline, i);
        opts.out = r.value;
        i = r.next;
        break;
      }
      case '--repo':
      case '-r': {
        const r = takeValue(name, inline, i);
        opts.repo = r.value;
        i = r.next;
        break;
      }
      case '--token':
      case '-t': {
        const r = takeValue(name, inline, i);
        opts.token = r.value;
        i = r.next;
        break;
      }
      case '--all':
        opts.all = true;
        break;
      case '--force':
      case '-f':
        opts.force = true;
        break;
      case '--list':
      case '-l':
        opts.list = true;
        break;
      case '--help':
      case '-h':
      case '/?':
        opts.help = true;
        break;
      default:
        throw new Error(`无法识别的参数: ${arg}（用 --help 查看用法）`);
    }
  }
  return opts;
}

function printHelp() {
  const def = DEFAULT_ASSETS.length ? DEFAULT_ASSETS.join(', ') : '(全部附件)';
  log('');
  log('  GitHub Release 附件下载工具');
  log('');
  log('  用法: node download.js [选项]');
  log('');
  log('  选项:');
  log(`      -v, --tag   <tag>       指定 Release 版本，如 v1.0.0（默认 ${DEFAULT_TAG}）`);
  log(`      -a, --asset <name>      指定附件名，可重复或用逗号分隔多个`);
  log(`          --all               下载该 Release 下的全部附件`);
  log(`      -o, --out   <dir>       输出目录，默认 ${DEFAULT_OUT}/（相对当前目录）`);
  log(`      -r, --repo  <o/r>       仓库，默认 ${DEFAULT_REPO || '(脚本内 DEFAULT_REPO 未设置)'}`);
  log(`      -t, --token <token>     GitHub Token，私有仓库才需要`);
  log(`      -l, --list              只列出该 Release 下的附件，不下载`);
  log(`      -f, --force             同名文件已存在时也重新下载`);
  log(`      -h, --help              显示本帮助`);
  log('');
  log(`  默认下载: ${def}`);
  log('  （改脚本顶部的 DEFAULT_REPO / DEFAULT_ASSETS 即可，写成 [] 等同于 --all）');
  log('');
  log('  示例:');
  log('      node download.js');
  log('      node download.js --tag v1.0.0');
  log('      node download.js --all');
  log('      node download.js -a a.zip,b.zip');
  log('      node download.js -a a.zip -a b.zip -o dist');
  log('      node download.js -r owner/repo');
  log('      node download.js --list');
  log('');
  log(`  退出码: 全部成功为 0；有文件下载失败或指定附件不存在为 1`);
  log('');
}

// 把各种写法统一成 { owner, repo }
// 支持: owner/repo、https://github.com/owner/repo、git@github.com:owner/repo.git
function parseRepo(value) {
  const cleaned = String(value)
    .trim()
    .replace(/\.git$/, '')
    .replace(/^.*github\.com[:/]/, '')
    .replace(/^\/+|\/+$/g, '');
  const parts = cleaned.split('/').filter(Boolean);
  if (parts.length < 2) return null;
  return { owner: parts[parts.length - 2], repo: parts[parts.length - 1] };
}

function resolveRepo(explicit) {
  // 1) 命令行显式指定
  if (explicit) {
    const r = parseRepo(explicit);
    if (!r) throw new Error(`--repo 格式应为 owner/repo，收到: ${explicit}`);
    return r;
  }

  // 2) 环境变量（CI 里 GITHUB_REPOSITORY 是现成的）
  for (const key of ['CLUB_REPO', 'GITHUB_REPOSITORY']) {
    const value = (process.env[key] || '').trim();
    if (!value) continue;
    const r = parseRepo(value);
    if (r) return r;
    throw new Error(`环境变量 ${key} 不是合法的 owner/repo: ${value}`);
  }

  // 3) 脚本里写死的默认仓库（本脚本常在空目录里运行，走的就是这一步）
  if (DEFAULT_REPO) {
    const r = parseRepo(DEFAULT_REPO);
    if (!r) throw new Error(`脚本内 DEFAULT_REPO 配置有误: ${DEFAULT_REPO}`);
    return r;
  }

  // 4) 兜底：从 git remote origin 解析（仅当 DEFAULT_REPO 留空时才会走到）
  let remote = '';
  try {
    remote = execFileSync('git', ['remote', 'get-url', 'origin'], {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    remote = '';
  }
  if (remote) {
    const r = parseRepo(remote);
    if (r) return r;
  }
  throw new Error('无法确定仓库，请用 --repo owner/repo 指定（或设置环境变量 CLUB_REPO）');
}

function resolveToken(explicit) {
  if (explicit) return explicit.trim();
  if (process.env.GITHUB_TOKEN) return process.env.GITHUB_TOKEN.trim();
  if (process.env.GH_TOKEN) return process.env.GH_TOKEN.trim();

  // 先看运行目录，再看脚本所在目录（脚本被拷走时也还能读到项目里的 token 文件）
  const seen = new Set();
  for (const dir of [CWD, ROOT]) {
    const tokenFile = path.join(dir, '.github-token');
    if (seen.has(tokenFile)) continue;
    seen.add(tokenFile);
    if (!fs.existsSync(tokenFile)) continue;
    const line = fs
      .readFileSync(tokenFile, 'utf8')
      .split(/\r?\n/)
      .map((s) => s.trim())
      .find(Boolean);
    if (line) return line;
  }
  return '';
}

function apiHeaders(token, accept = 'application/vnd.github+json') {
  const headers = {
    Accept: accept,
    'X-GitHub-Api-Version': API_VER,
    'User-Agent': USER_AGENT,
  };
  if (token) headers.Authorization = `Bearer ${token}`;
  return headers;
}

async function apiGet(url, token) {
  const res = await fetch(url, {
    headers: apiHeaders(token),
    signal: AbortSignal.timeout(30000),
  });
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  return { ok: res.ok, status: res.status, body };
}

// ---------------------------------------------------------------- 下载实现

async function downloadTo({ url, headers, dest, expectedSize, label }) {
  const tmp = `${dest}.part`;
  await fsp.rm(tmp, { force: true });

  let res;
  try {
    res = await fetch(url, {
      headers,
      redirect: 'follow',
      // 兜底超时，避免连接卡死时一直挂着
      signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS),
    });
  } catch (err) {
    throw new Error(`网络请求失败: ${describeFetchError(err)}`);
  }

  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(
      `下载失败 HTTP ${res.status} ${res.statusText}${detail ? ` — ${detail.slice(0, 200)}` : ''}`,
    );
  }
  if (!res.body) throw new Error('响应没有内容体');

  const total = Number(res.headers.get('content-length')) || expectedSize || 0;
  const hash = crypto.createHash('sha256');
  const tty = Boolean(process.stdout.isTTY);
  const startedAt = Date.now();
  let received = 0;

  const tap = new Transform({
    transform(chunk, _enc, cb) {
      hash.update(chunk);
      received += chunk.length;
      if (tty) {
        const pct = total ? `${((received / total) * 100).toFixed(1)}%` : '--';
        process.stdout.write(
          `\r  ${label}  ${pct}  ${human(received)}${total ? ` / ${human(total)}` : ''}   `,
        );
      }
      cb(null, chunk);
    },
  });

  try {
    await pipeline(Readable.fromWeb(res.body), tap, fs.createWriteStream(tmp));
  } catch (err) {
    await fsp.rm(tmp, { force: true });
    throw new Error(`下载中断: ${describeFetchError(err)}`);
  }
  if (tty) process.stdout.write(`\r${' '.repeat(72)}\r`);

  await fsp.rename(tmp, dest);
  return {
    size: received,
    sha256: hash.digest('hex'),
    seconds: (Date.now() - startedAt) / 1000,
  };
}

// 网络不稳时自动重试
async function downloadWithRetry(params) {
  let lastErr;
  for (let i = 1; i <= DOWNLOAD_ATTEMPTS; i += 1) {
    try {
      return await downloadTo(params);
    } catch (err) {
      lastErr = err;
      if (i < DOWNLOAD_ATTEMPTS) {
        log(`  [重试 ${i}/${DOWNLOAD_ATTEMPTS - 1}] ${err.message}`);
        await sleep(1000 * i);
      }
    }
  }
  throw lastErr;
}

// ---------------------------------------------------------------- 备份 / 恢复

// 创建（若已存在则先删除再新建）一个空的 backup 目录，只执行一次
async function ensureFreshBackupDir(dir, state) {
  if (state.created) return;
  await fsp.rm(dir, { recursive: true, force: true });
  await fsp.mkdir(dir, { recursive: true });
  state.created = true;
}

// 把目标文件备份进 backup 目录（只备份需要下载的文件）
async function backupFile(src, backupDir, state) {
  await ensureFreshBackupDir(backupDir, state);
  const bk = path.join(backupDir, path.basename(src));
  await fsp.copyFile(src, bk);
  return bk;
}

// 下载失败后从 backup 目录恢复原文件
async function restoreFile(dest, backupDir) {
  const bk = path.join(backupDir, path.basename(dest));
  if (!fs.existsSync(bk)) return false;
  await fsp.copyFile(bk, dest);
  return true;
}

// ---------------------------------------------------------------- 主流程

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    printHelp();
    return 0;
  }

  const { owner, repo } = resolveRepo(opts.repo);
  const token = resolveToken(opts.token);
  const repoLabel = `${owner}/${repo}`;

  // 1) 找到 Release
  const relUrl = opts.tag
    ? `${API_BASE}/repos/${owner}/${repo}/releases/tags/${encodeURIComponent(opts.tag)}`
    : `${API_BASE}/repos/${owner}/${repo}/releases/latest`;

  let release;
  try {
    const r = await apiGet(relUrl, token);
    if (!r.ok) {
      if (r.status === 404) {
        throw new Error(
          opts.tag
            ? `仓库 ${repoLabel} 下找不到 Release: ${opts.tag}`
            : `仓库 ${repoLabel} 不存在（或名字写错），也可能还没有已发布的 Release`,
        );
      }
      if (r.status === 401 || r.status === 403) {
        const bodyMsg = (r.body && r.body.message) || '';
        const isRateLimit =
          r.status === 403 && /rate\s*limit/i.test(`${bodyMsg} ${(r.body && r.body.documentation_url) || ''}`);
        if (isRateLimit) {
          throw new Error(
            `GitHub API 已限流（HTTP 403）！${token ? '' : '未认证每个 IP 每小时约 60 次，'}你多半在这个小时用完了额度。\n` +
              `        解决方法：稍等一个小时再跑，或配置 Token（-t 或环境变量 GITHUB_TOKEN）把额度提到 5000 次/小时。\n` +
              `        详情: ${bodyMsg || 'rate limit exceeded'}`,
          );
        }
        throw new Error(
          `访问被拒绝（HTTP ${r.status}）${token ? '，Token 可能无效或权限不足' : '。这是公开仓库却仍返回 403，通常是临时限流或网络/代理因素，建议配置 Token 后再试'}`,
        );
      }
      throw new Error(
        `查询 Release 失败: HTTP ${r.status} ${r.body && r.body.message ? r.body.message : ''}`,
      );
    }
    release = r.body;
  } catch (err) {
    if (err.name === 'TimeoutError') throw new Error('查询 Release 超时，请检查网络');
    throw err;
  }

  const assets = Array.isArray(release.assets) ? release.assets : [];

  // 2) 选出要下载的附件：--all（或 DEFAULT_ASSETS 为空）时取全部，否则按名字匹配
  const targets = [];
  const missing = [];
  const taken = new Set();

  if (opts.all || opts.assets.length === 0) {
    for (const a of assets) {
      if (!taken.has(a.name)) {
        targets.push(a);
        taken.add(a.name);
      }
    }
  } else {
    for (const name of opts.assets) {
      if (taken.has(name)) continue;
      const hit = assets.find((a) => a.name === name);
      if (hit) {
        targets.push(hit);
        taken.add(hit.name);
      } else {
        missing.push(name);
      }
    }
  }

  log(`仓库    : ${repoLabel}`);
  log(
    `版本    : ${release.tag_name}${
      release.name && release.name !== release.tag_name ? `  (${release.name})` : ''
    }`,
  );
  log(`附件数  : ${assets.length}`);

  // 3) --list 只列不下载
  if (opts.list) {
    log('');
    log('  附件列表:');
    for (const a of assets) {
      const mark = taken.has(a.name) ? '[将下载]' : '        ';
      log(`    ${mark} ${a.name}   ${human(a.size)}   下载 ${a.download_count || 0} 次`);
    }
    if (!assets.length) log('    (该 Release 没有任何附件)');
    log('');
    return 0;
  }

  // 4) 一个都没匹配上：直接报错
  if (!targets.length) {
    log('');
    log(`[错误] 在 Release ${release.tag_name} 里没有找到任何要下载的附件`);
    if (assets.length) {
      log('       可用的附件:');
      for (const a of assets) log(`         - ${a.name}`);
      log('       用 --asset <名称> 指定，或加 --list 查看');
    } else {
      log('       该 Release 没有任何附件');
    }
    return 1;
  }

  // 5) 目标目录（相对路径基于运行时的当前目录）
  const outDir = path.resolve(CWD, opts.out);
  await fsp.mkdir(outDir, { recursive: true });

  log('');
  log(`输出目录: ${outDir}`);
  log(`待下载  : ${targets.length} 个文件`);
  for (const a of targets) log(`    - ${a.name}   ${human(a.size)}`);
  if (missing.length) {
    log('');
    log(`[警告] 以下附件在 Release ${release.tag_name} 里不存在，已跳过:`);
    for (const n of missing) log(`         - ${n}`);
  }

  // 6) 逐个下载
  const results = [];
  const backupDir = path.join(outDir, 'backup');
  const backupState = { created: false };
  for (let idx = 0; idx < targets.length; idx += 1) {
    const asset = targets[idx];
    const dest = path.join(outDir, asset.name);
    log('');
    log(`[${idx + 1}/${targets.length}] ${asset.name}`);

    // 已存在且大小一致就跳过
    if (!opts.force && fs.existsSync(dest)) {
      const local = (await fsp.stat(dest)).size;
      if (asset.size && local === asset.size) {
        log(`  [跳过] 已存在且大小一致 (${human(local)})，需要重下请加 --force`);
        results.push({ name: asset.name, dest, status: 'skipped' });
        continue;
      }
      log(`  [提示] 本地大小不一致（${human(local)} != ${human(asset.size)}），重新下载`);
    }

    // 下载前：当前目录已有同名文件则备份到 backup/（只备份需要下载的文件）
    if (fs.existsSync(dest)) {
      try {
        const bk = await backupFile(dest, backupDir, backupState);
        log(`  [备份] 原文件已备份到 backup/${path.basename(bk)}`);
      } catch (err) {
        log(`  [警告] 备份失败，继续下载: ${err.message}`);
      }
    }

    // 有 Token 时走 API（私有仓库也可用），否则用公开的 browser_download_url
    const useApi = Boolean(token);
    const url = useApi
      ? `${API_BASE}/repos/${owner}/${repo}/releases/assets/${asset.id}`
      : asset.browser_download_url;
    const headers = apiHeaders(
      token,
      useApi ? 'application/octet-stream' : 'application/vnd.github+json',
    );

    try {
      const r = await downloadWithRetry({
        url,
        headers,
        dest,
        expectedSize: asset.size,
        label: asset.name,
      });

      // 校验 GitHub 提供的 sha256（有这个字段时才校验）
      if (asset.digest && /^sha256:/i.test(asset.digest)) {
        const expected = asset.digest.slice(asset.digest.indexOf(':') + 1).toLowerCase();
        if (expected !== r.sha256) {
          await fsp.rm(dest, { force: true });
          const restored = await restoreFile(dest, backupDir);
          log(`  [错误] sha256 校验失败，文件已${restored ? '恢复' : '删除'}`);
          log(`         期望: ${expected}`);
          log(`         实际: ${r.sha256}`);
          results.push({ name: asset.name, dest, status: 'failed' });
          continue;
        }
      }

      log(`  [完成] ${human(r.size)}  耗时 ${r.seconds.toFixed(2)} 秒`);
      log(`         sha256 ${r.sha256}${asset.digest ? '  (已校验)' : ''}`);
      results.push({ name: asset.name, dest, status: 'ok' });
    } catch (err) {
      log(`  [失败] ${err.message}`);
      const restored = await restoreFile(dest, backupDir);
      if (restored) log(`  [恢复] 已恢复 backup 中的原文件`);
      results.push({ name: asset.name, dest, status: 'failed' });
    }
  }

  // 7) 汇总
  const okList = results.filter((r) => r.status === 'ok');
  const skipList = results.filter((r) => r.status === 'skipped');
  const failList = results.filter((r) => r.status === 'failed');

  log('');
  log('===========================================================');
  log(`  下载 ${okList.length} 个，跳过 ${skipList.length} 个，失败 ${failList.length} 个`);
  for (const it of results) {
    const tag = it.status === 'ok' ? 'OK  ' : it.status === 'skipped' ? '跳过' : '失败';
    log(`    [${tag}] ${it.name}`);
  }
  if (missing.length) log(`    缺失 ${missing.length} 个: ${missing.join(', ')}`);
  log(`  目录: ${outDir}`);
  log('===========================================================');
  log('');

  return failList.length || missing.length ? 1 : 0;
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err) => {
    log('');
    log(`[错误] ${err.message}`);
    process.exitCode = 1;
  });
