#!/usr/bin/env node
// ez-folder-peer CLI 감독자 — 데몬 + 헤드리스 peer 를 기동·감독.
// (옵션) 하루 1회 자동업데이트(config.toml [update].enabled). 디폴트 OFF.
// 자세한 사용법: README.dev.md
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
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
  }
  return a;
}

const HELP = `ez-folder-peer — Linux 헤드리스 원격 호스트 peer

사용: ez-folder-peer [옵션]
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
