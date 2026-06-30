#!/usr/bin/env node
// ez-folder-peer CLI 감독자 — 데몬 + 헤드리스 peer 를 기동·감독.
// (옵션) 하루 1회 자동업데이트(config.toml [update].enabled). 디폴트 OFF.
// 자세한 사용법: README.dev.md
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { paths, dataDir, readConfig, exists, log } from '../lib/shared.mjs';
import { checkAndUpdate } from '../lib/updater.mjs';

function parseArgs(argv) {
  const a = { autoUpdate: false };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    const next = () => argv[++i];
    if (k === '--token') a.token = next();
    else if (k === '--data-dir') a.dataDir = next();
    else if (k === '--port') a.port = next();
    else if (k === '--server') a.server = next();
    else if (k === '--account') a.account = next();
    else if (k === '--daemon-bin') a.daemonBin = next();
    else if (k === '--auto-update') a.autoUpdate = true;
    else if (k === '--help' || k === '-h') a.help = true;
    // 첫 비-플래그 토큰 = 서브커맨드(예: url). 플래그 값은 next() 가 이미 소비.
    else if (!k.startsWith('-') && !a.command) a.command = k;
  }
  return a;
}

const HELP = `ez-folder-peer — Linux 헤드리스 원격 호스트 peer

사용: ez-folder-peer [명령] [옵션]

명령:
  (없음)               데몬 + peer 를 기동·감독(상주). 기동 시 원격 접속 주소 출력.
  url                  이 PC 의 원격 접속 주소만 출력하고 종료(데몬을 새로 띄우지 않음).

옵션:
  --token <T>          데몬 토큰 (없으면 config.toml token / EZFD_DAEMON_TOKEN)
  --data-dir <DIR>     데몬 data_dir (기본 ~/.ez-folder-daemon)
  --port <N>           데몬 포트 (기본 59100)
  --server <URL>       시그널링 서버 (기본 https://ez-folder.bbo-odd.com)
  --account user:pass  최초 1회 관리자 계정 생성
  --daemon-bin <PATH>  데몬 바이너리 경로 override (기본 vendor/ez-folder-daemon)
  --auto-update        이번 실행만 자동업데이트 ON (영속 설정은 config.toml [update])
  -h, --help
`;

const DAY_MS = 24 * 60 * 60 * 1000;
const READYZ_TIMEOUT_MS = 30_000;

let shuttingDown = false;
let daemonChild = null;
let peerChild = null;

async function waitReadyz(base, token) {
  const deadline = Date.now() + READYZ_TIMEOUT_MS;
  const headers = token ? { Authorization: `Bearer ${token}` } : {};
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${base}/v1/readyz`, { headers });
      if (r.ok) { const j = await r.json(); if (j.ready) return true; }
    } catch { /* not up yet */ }
    await sleep(500);
  }
  return false;
}

function startDaemon(cfg) {
  log('daemon-spawn', { bin: cfg.daemonBin, port: cfg.port });
  const child = spawn(cfg.daemonBin, [], {
    env: {
      ...process.env,
      EZFD_TOKEN: cfg.token,
      EZFD_DATA_DIR: cfg.dataDir,
      EZFD_HOST: '127.0.0.1',
      EZFD_PORT_OVERRIDE: String(cfg.port),
    },
    stdio: 'inherit',
  });
  child.on('exit', (code, sig) => {
    daemonChild = null;
    if (shuttingDown) return;
    log('daemon-exit', { code, sig, action: 'restart in 3s' });
    setTimeout(() => { if (!shuttingDown) bootDaemon(cfg); }, 3000);
  });
  return child;
}

function startPeer(cfg) {
  log('peer-spawn', { bundle: cfg.peerBundle });
  const child = spawn(process.execPath, [cfg.peerBundle], {
    env: {
      ...process.env,
      EZFD_DAEMON_TOKEN: cfg.token,
      EZFD_DAEMON_BASE: cfg.base,
      EZ_REMOTE_SERVER: cfg.server,
    },
    stdio: 'inherit',
  });
  child.on('exit', (code, sig) => {
    peerChild = null;
    if (shuttingDown) return;
    log('peer-exit', { code, sig, action: 'restart in 3s' });
    setTimeout(() => { if (!shuttingDown) { peerChild = startPeer(cfg); } }, 3000);
  });
  return child;
}

async function bootDaemon(cfg) {
  daemonChild = startDaemon(cfg);
  const ok = await waitReadyz(cfg.base, cfg.token);
  if (!ok) { log('daemon-not-ready', { base: cfg.base, hint: '토큰/포트 확인' }); }
  return ok;
}

async function maybeCreateAccount(cfg) {
  if (!cfg.account) return;
  const idx = cfg.account.indexOf(':');
  if (idx < 1) { log('account-bad', { hint: '--account user:pass 형식' }); return; }
  const username = cfg.account.slice(0, idx);
  const password = cfg.account.slice(idx + 1);
  try {
    const r = await fetch(`${cfg.base}/v1/auth/account`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${cfg.token}` },
      body: JSON.stringify({ username, password }),
    });
    log('account-set', { username, status: r.status });
  } catch (e) {
    log('account-fail', { err: String(e?.message || e) });
  }
}

function stopChild(child) {
  return new Promise((resolve) => {
    if (!child) return resolve();
    child.removeAllListeners('exit');
    child.once('exit', resolve);
    try { child.kill('SIGTERM'); } catch { resolve(); }
    setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* */ } resolve(); }, 5000);
  });
}

async function restartAll(cfg) {
  log('restart-all', { reason: 'update applied' });
  await Promise.all([stopChild(peerChild), stopChild(daemonChild)]);
  peerChild = null; daemonChild = null;
  await bootDaemon(cfg);
  await maybeCreateAccount(cfg);
  peerChild = startPeer(cfg);
}

function startUpdateLoop(cfg) {
  const run = async () => {
    if (shuttingDown) return;
    try {
      const res = await checkAndUpdate({ channel: cfg.channel });
      if (res.updated) { log('update-found', res); await restartAll(cfg); }
      else log('update-none', res);
    } catch (e) {
      log('update-error', { err: String(e?.message || e) });
    }
  };
  setTimeout(run, 10_000); // 부팅 직후 1회(데몬 안정화 대기)
  setInterval(run, DAY_MS); // 이후 하루 1회
  log('update-loop', { enabled: true, channel: cfg.channel, every: '24h' });
}

// 데몬과 동일한 machine-code 계산(http.rs machine_code): Blake2b512(hostname) 앞 8바이트 대문자 hex.
// 데몬이 안 떠 있을 때만 쓰는 폴백 — 데몬의 env(COMPUTERNAME/HOSTNAME)와 같아야 정확히 일치한다.
function localMachineCode() {
  const host = process.env.COMPUTERNAME || process.env.HOSTNAME || 'ez-folder';
  const h = createHash('blake2b512').update(host).digest();
  return Array.from(h.subarray(0, 8)).map((b) => b.toString(16).padStart(2, '0').toUpperCase()).join('');
}

// deviceId 확보: 떠 있는 데몬에 물어보면 정확(데몬을 새로 띄우지 않음). 없으면 로컬 계산 폴백.
async function resolveDeviceId(cfg) {
  if (cfg.token) {
    try {
      const r = await fetch(`${cfg.base}/v1/license/machine-code`, { headers: { Authorization: `Bearer ${cfg.token}` } });
      if (r.ok) { const j = await r.json(); if (j.machineCode) return { id: j.machineCode, source: 'daemon' }; }
    } catch { /* 데몬 미가동 → 로컬 폴백 */ }
  }
  return { id: localMachineCode(), source: 'local' };
}

// `ez-folder-peer url` — 이 PC 의 영구 원격 주소만 출력하고 종료.
// 시그널링의 읽기전용 조회(device-slug)를 써서 register(=secret 회전, 상주 peer 깨짐)를 피한다.
async function showUrl(cfg) {
  const { id: deviceId, source } = await resolveDeviceId(cfg);
  const lookup = `${cfg.server}/api/v1/remote/device-slug/${encodeURIComponent(deviceId)}`;
  let slug;
  try {
    const r = await fetch(lookup);
    if (r.status === 404) {
      log('url-unregistered', { hint: 'peer 를 한 번 실행(ez-folder-peer --token … --account …)해 등록한 뒤 다시 시도' });
      process.exit(2);
    }
    const env = await r.json();
    if (!env.ok || !env.data?.slug) throw new Error(env.error?.message || 'slug 조회 실패');
    slug = env.data.slug;
  } catch (e) {
    log('url-fail', { err: String(e?.message || e), hint: `시그널링 서버 연결 확인(--server ${cfg.server})` });
    process.exit(1);
  }
  process.stdout.write(
    `\n[ez-folder-peer] 이 PC 의 원격 접속 주소 (고정 — 항상 같습니다)\n` +
    `  desktop: ${cfg.server}/d/${slug}\n` +
    `  mobile : ${cfg.server}/m/${slug}\n` +
    (source === 'local' ? `  ※ 데몬 미가동 → 호스트명으로 계산. 데몬 실행 중에 다시 돌리면 정확합니다.\n` : '') +
    `\n`,
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) { process.stdout.write(HELP); return; }

  const fileCfg = await readConfig();
  const p = paths();
  const cfg = {
    token: args.token || process.env.EZFD_DAEMON_TOKEN || fileCfg.token || '',
    dataDir: args.dataDir || dataDir(),
    port: args.port || process.env.EZFD_PORT_OVERRIDE || '59100',
    server: (args.server || process.env.EZ_REMOTE_SERVER || 'https://ez-folder.bbo-odd.com').replace(/\/$/, ''),
    account: args.account,
    daemonBin: args.daemonBin || p.daemonBin,
    peerBundle: p.peerBundle,
    autoUpdate: args.autoUpdate || fileCfg.update.enabled,
    channel: fileCfg.update.channel || 'latest',
  };
  cfg.base = `http://127.0.0.1:${cfg.port}`;

  // url 명령은 데몬/번들 없이도 동작(상주 안 함) — fatal 체크 전에 분기.
  if (args.command === 'url') { await showUrl(cfg); return; }

  if (!cfg.token) { log('fatal', { msg: '토큰 없음 — --token / EZFD_DAEMON_TOKEN / config.toml token 중 하나 필요' }); process.exit(1); }
  if (!(await exists(cfg.daemonBin))) {
    log('fatal', { msg: `데몬 바이너리 없음: ${cfg.daemonBin}`, hint: 'npm 재설치(postinstall 다운로드) 또는 --daemon-bin' });
    process.exit(1);
  }
  if (!(await exists(cfg.peerBundle))) {
    log('fatal', { msg: `peer 번들 없음: ${cfg.peerBundle}` });
    process.exit(1);
  }

  const stop = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    log('shutdown', {});
    await Promise.all([stopChild(peerChild), stopChild(daemonChild)]);
    process.exit(0);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);

  await bootDaemon(cfg);
  await maybeCreateAccount(cfg);
  peerChild = startPeer(cfg);
  if (cfg.autoUpdate) startUpdateLoop(cfg);
  else log('update-loop', { enabled: false, hint: 'config.toml [update].enabled=true 로 켬' });
}

main().catch((e) => { log('fatal', { err: e?.stack || String(e) }); process.exit(1); });
