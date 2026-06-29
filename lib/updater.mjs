// 옵트인 자동업데이트 — 하루 1회(호출자가 24h 스케줄). sha256 검증 + 원자교체.
// 호출자(bin)가 checkAndUpdate() 결과 updated=true 면 자식(데몬+peer) graceful 재기동.
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { readFile, writeFile } from 'node:fs/promises';
import {
  paths, pkgVersion, repoCoords, daemonAssetName,
  ghJson, downloadTo, sha256File, parseChecksums,
  rename, chmod, exists, log,
} from './shared.mjs';

/** "1.2.3" 비교 — a > b ? (prerelease 태그는 단순 무시). */
export function semverGt(a, b) {
  const norm = (s) => String(s).replace(/^v/, '').split('-')[0].split('.').map((n) => parseInt(n, 10) || 0);
  const [a1, a2, a3] = norm(a); const [b1, b2, b3] = norm(b);
  if (a1 !== b1) return a1 > b1;
  if (a2 !== b2) return a2 > b2;
  return a3 > b3;
}

async function currentVersion() {
  const p = paths();
  if (await exists(p.versionFile)) return (await readFile(p.versionFile, 'utf8')).trim();
  return pkgVersion();
}

async function pickRelease(owner, repo, channel) {
  if (channel === 'prerelease') {
    const list = await ghJson(`https://api.github.com/repos/${owner}/${repo}/releases?per_page=5`);
    if (Array.isArray(list) && list.length) return list[0];
    throw new Error('릴리스 없음');
  }
  return ghJson(`https://api.github.com/repos/${owner}/${repo}/releases/latest`);
}

/** 한 에셋을 temp 로 받아 sha256 검증. {tmp} 반환. */
async function fetchVerified(asset, wantSha) {
  const tmp = join(tmpdir(), `ezfd-up-${asset.name}-${process.pid}`);
  await downloadTo(asset.browser_download_url, tmp);
  if (wantSha) {
    const got = await sha256File(tmp);
    if (got !== wantSha) throw new Error(`${asset.name} sha256 불일치 — 업데이트 취소(변조 의심)`);
  }
  return tmp;
}

/**
 * 새 버전 확인 → 있으면 데몬+peer 다운로드·검증·원자교체.
 * @returns {Promise<{updated:boolean, from:string, to?:string, reason?:string}>}
 */
export async function checkAndUpdate({ channel = 'latest' } = {}) {
  const from = await currentVersion();
  const { owner, repo } = await repoCoords();
  let rel;
  try {
    rel = await pickRelease(owner, repo, channel);
  } catch (e) {
    return { updated: false, from, reason: `release-check-fail: ${e?.message || e}` };
  }
  const to = String(rel.tag_name || '').replace(/^v/, '');
  if (!to || !semverGt(to, from)) return { updated: false, from };

  const assets = rel.assets || [];
  const daemonName = daemonAssetName();
  const daemonA = assets.find((a) => a.name === daemonName);
  const peerA = assets.find((a) => a.name === 'peer.mjs');
  const sumsA = assets.find((a) => a.name === 'checksums.txt');
  if (!daemonA || !peerA) return { updated: false, from, reason: `에셋 누락(${daemonName}/peer.mjs)` };

  let sums = {};
  if (sumsA) {
    const st = join(tmpdir(), `ezfd-up-sums-${process.pid}.txt`);
    await downloadTo(sumsA.browser_download_url, st);
    sums = parseChecksums(await readFile(st, 'utf8'));
  }

  // 둘 다 받아 검증 후에야 교체(부분 적용 방지).
  const daemonTmp = await fetchVerified(daemonA, sums[daemonName]);
  const peerTmp = await fetchVerified(peerA, sums['peer.mjs']);

  const p = paths();
  await rename(daemonTmp, p.daemonBin);
  await chmod(p.daemonBin, 0o755);
  await rename(peerTmp, p.peerBundle);
  await writeFile(p.versionFile, to, 'utf8');
  log('update-applied', { from, to, channel });
  return { updated: true, from, to };
}
