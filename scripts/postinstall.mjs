// postinstall — 현재 arch 의 데몬 바이너리를 GitHub Releases 에서 받아 vendor/ 에 둔다.
// sha256 검증 필수. 실패해도 npm install 자체는 깨지 않고(경고만) CLI 가 부팅 시 재확인.
// 건너뛰기: EZ_SKIP_DAEMON_DOWNLOAD=1 (오프라인/CI).
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  paths, pkgVersion, repoCoords, daemonAssetName,
  ghJson, downloadTo, sha256File, parseChecksums, exists,
  rename, chmod, mkdir, log,
} from '../lib/shared.mjs';
import { readFile } from 'node:fs/promises';

async function main() {
  if (process.env.EZ_SKIP_DAEMON_DOWNLOAD === '1') {
    log('postinstall-skip', { reason: 'EZ_SKIP_DAEMON_DOWNLOAD=1' });
    return;
  }
  if (process.platform !== 'linux') {
    log('postinstall-skip', { reason: `non-linux (${process.platform}) — 데몬 바이너리 미배포` });
    return;
  }

  const asset = daemonAssetName();
  const { owner, repo } = await repoCoords();
  const version = await pkgVersion();
  const p = paths();

  // 이미 같은 버전 설치돼 있으면 스킵.
  if (await exists(p.daemonBin) && await exists(p.versionFile)) {
    const have = (await readFile(p.versionFile, 'utf8')).trim();
    if (have === version) { log('postinstall-have', { version }); return; }
  }

  // 릴리스 선택: v{version} 태그 → 없으면 latest.
  let rel;
  try {
    rel = await ghJson(`https://api.github.com/repos/${owner}/${repo}/releases/tags/v${version}`);
  } catch {
    log('postinstall-tag-miss', { tag: `v${version}`, fallback: 'latest' });
    rel = await ghJson(`https://api.github.com/repos/${owner}/${repo}/releases/latest`);
  }
  const assets = rel.assets || [];
  const daemonA = assets.find((a) => a.name === asset);
  const sumsA = assets.find((a) => a.name === 'checksums.txt');
  if (!daemonA) throw new Error(`릴리스 ${rel.tag_name} 에 ${asset} 에셋 없음`);

  const tmp = join(tmpdir(), `ezfd-daemon-${process.pid}`);
  await downloadTo(daemonA.browser_download_url, tmp);

  // 무결성 검증(checksums.txt 있을 때 필수).
  if (sumsA) {
    const sumsTmp = join(tmpdir(), `ezfd-sums-${process.pid}.txt`);
    await downloadTo(sumsA.browser_download_url, sumsTmp);
    const want = parseChecksums(await readFile(sumsTmp, 'utf8'))[asset];
    const got = await sha256File(tmp);
    if (!want) throw new Error(`checksums.txt 에 ${asset} 항목 없음`);
    if (want !== got) throw new Error(`sha256 불일치 — 변조 의심 (want=${want} got=${got})`);
    log('postinstall-verified', { asset, sha256: got.slice(0, 12) });
  } else {
    log('postinstall-no-checksums', { warn: 'checksums.txt 없음 — 검증 생략(권장 X)' });
  }

  await mkdir(p.vendorDir, { recursive: true });
  await rename(tmp, p.daemonBin);
  await chmod(p.daemonBin, 0o755);
  const { writeFile } = await import('node:fs/promises');
  await writeFile(p.versionFile, version, 'utf8');
  log('postinstall-done', { asset, version, dest: p.daemonBin });
}

main().catch((e) => {
  // install 은 깨지 않는다 — CLI 가 부팅 시 데몬 부재를 안내·재시도.
  log('postinstall-fail', { err: String(e?.message || e), hint: '부팅 시 재시도 또는 수동 배치' });
  process.exit(0);
});
