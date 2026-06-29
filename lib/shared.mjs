// ez-folder-peer 공용 헬퍼 — postinstall·updater·bin 공유.
// 의존성 0(Node 내장만): paths / config.toml 파싱 / 다운로드 / sha256 / GitHub API.
import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, readFile, rename, chmod, stat } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { homedir } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));

/** 패키지 루트(= lib/..) 와 주요 경로. */
export function paths() {
  const root = resolve(HERE, '..');
  const vendorDir = join(root, 'vendor');
  return {
    root,
    vendorDir,
    daemonBin: join(vendorDir, 'ez-folder-daemon'),
    peerBundle: join(root, 'dist', 'peer.mjs'),
    versionFile: join(vendorDir, '.installed-version'),
    packageJson: join(root, 'package.json'),
  };
}

/** 데몬 data_dir (EZFD_DATA_DIR > ~/.ez-folder-daemon). */
export function dataDir() {
  const env = process.env.EZFD_DATA_DIR;
  return env && env.trim() ? env.trim() : join(homedir(), '.ez-folder-daemon');
}

/** package.json 의 version. */
export async function pkgVersion() {
  try {
    const j = JSON.parse(await readFile(paths().packageJson, 'utf8'));
    return j.version || '0.0.0';
  } catch {
    return '0.0.0';
  }
}

/** repository.url → {owner, repo}. 미해석 시 ldlive/ez-folder-dist 폴백. */
export async function repoCoords() {
  try {
    const j = JSON.parse(await readFile(paths().packageJson, 'utf8'));
    const m = String(j.repository?.url || '').match(/github\.com[/:]([^/]+)\/([^/.]+)/);
    if (m) return { owner: m[1], repo: m[2] };
  } catch { /* fall through */ }
  return { owner: 'ldlive', repo: 'ez-folder-dist' };
}

/** 현재 arch 의 데몬 릴리스 에셋 이름. linux 만 지원. */
export function daemonAssetName() {
  const arch = process.arch === 'arm64' ? 'arm64' : process.arch === 'x64' ? 'x64' : null;
  if (process.platform !== 'linux' || !arch) {
    throw new Error(`지원하지 않는 플랫폼: ${process.platform}/${process.arch} (linux x64/arm64 만)`);
  }
  return `ez-folder-daemon-linux-${arch}`;
}

/** 최소 TOML 리더 — config.toml 에서 token + [update] 만 뽑는다(외부 dep 회피). */
export function parseConfigToml(text) {
  const out = { token: '', update: { enabled: false, channel: 'latest' } };
  let section = '';
  for (const raw of String(text).split(/\r?\n/)) {
    const line = raw.replace(/#.*$/, '').trim();
    if (!line) continue;
    const sec = line.match(/^\[([^\]]+)\]$/);
    if (sec) { section = sec[1].trim(); continue; }
    const kv = line.match(/^([A-Za-z0-9_]+)\s*=\s*(.+)$/);
    if (!kv) continue;
    const key = kv[1];
    let val = kv[2].trim();
    const str = val.match(/^"([^"]*)"$/);
    if (str) val = str[1];
    if (section === '' && key === 'token') out.token = val;
    else if (section === 'update' && key === 'enabled') out.update.enabled = val === 'true';
    else if (section === 'update' && key === 'channel') out.update.channel = val;
  }
  return out;
}

/** config.toml 읽어 파싱(없으면 기본값). */
export async function readConfig() {
  try {
    const txt = await readFile(join(dataDir(), 'config.toml'), 'utf8');
    return parseConfigToml(txt);
  } catch {
    return { token: '', update: { enabled: false, channel: 'latest' } };
  }
}

const UA = { 'User-Agent': 'ez-folder-peer', Accept: 'application/vnd.github+json' };

/** GitHub API JSON GET. */
export async function ghJson(url) {
  const res = await fetch(url, { headers: UA });
  if (!res.ok) throw new Error(`GET ${url} → ${res.status}`);
  return res.json();
}

/** 파일 다운로드(리다이렉트는 fetch 가 따라감). dest 디렉토리는 생성. */
export async function downloadTo(url, dest) {
  await mkdir(dirname(dest), { recursive: true });
  const res = await fetch(url, { headers: { 'User-Agent': 'ez-folder-peer' } });
  if (!res.ok || !res.body) throw new Error(`download ${url} → ${res.status}`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(dest));
}

/** 파일 sha256(hex). */
export async function sha256File(path) {
  const buf = await readFile(path);
  return createHash('sha256').update(buf).digest('hex');
}

/** checksums.txt("<sha>  <name>" 줄들) → {name: sha}. */
export function parseChecksums(text) {
  const map = {};
  for (const line of String(text).split(/\r?\n/)) {
    const m = line.trim().match(/^([0-9a-fA-F]{64})\s+\*?(.+)$/);
    if (m) map[m[2].trim()] = m[1].toLowerCase();
  }
  return map;
}

/** 파일 존재 여부. */
export async function exists(p) {
  try { await stat(p); return true; } catch { return false; }
}

export { rename, chmod, mkdir };

/** 태그된 로그 한 줄(관측성 계약 — [peer-cli][이벤트]). */
export function log(event, obj) {
  const tag = `[peer-cli][${event}]`;
  process.stdout.write(obj ? `${tag} ${JSON.stringify(obj)}\n` : `${tag}\n`);
}
