// <define:import.meta.env>
var define_import_meta_env_default = { DEV: false, PROD: true, MODE: "production" };

// src/peer.ts
import WS from "ws";
import * as ndc from "node-datachannel/polyfill";

// ../quasar/src/services/remote/SignalingClient.ts
import { Centrifuge } from "centrifuge";
var SignalingClient = class _SignalingClient {
  server;
  base;
  slug;
  session;
  role;
  secret;
  fetchImpl;
  cf = null;
  sub = null;
  queue = [];
  nextId = 0;
  connecting = null;
  /** Centrifugo 연결 생존 여부. 'connected'/'disconnected' 이벤트로 갱신 — peer self-heal 판정에 사용. */
  connected = false;
  /** nextMessages() 대기자 — publication 도착 시 즉시 깨운다(고정 폴링 지연 제거). */
  waiters = [];
  constructor(opts) {
    this.server = opts.server.replace(/\/$/, "");
    this.base = this.server + "/api/v1/remote";
    this.slug = opts.slug;
    this.session = opts.session;
    this.role = opts.role;
    this.secret = opts.secret;
    this.fetchImpl = opts.fetchImpl ?? globalThis.fetch.bind(globalThis);
  }
  /** 정적: 기기 등록 → {slug, registrationSecret}. (매니저 RemotePeerHost / 앱 피어가 호출) */
  static async register(server, deviceId, fetchImpl = globalThis.fetch.bind(globalThis)) {
    const base = server.replace(/\/$/, "") + "/api/v1/remote";
    const res = await fetchImpl(`${base}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ deviceId })
    });
    const env = await res.json();
    if (!env.ok || !env.data) throw new Error(env.error?.message ?? "register failed");
    return env.data;
  }
  /** 정적: 슬러그 온라인 여부(폰이 접속 전 확인). */
  static async resolve(server, slug, fetchImpl = globalThis.fetch.bind(globalThis)) {
    return (await _SignalingClient.resolveDetailed(server, slug, fetchImpl)).online;
  }
  /**
   * 정적: resolve + 사람이 읽는 진단 한 줄. online=false 의 *사유* 구분:
   *  - fetch 실패 → 시그널링 서버 미도달
   *  - 응답 online=false → 이 PC(slug)가 피어로 접속 안 됨(미실행·다른 slug)
   */
  static async resolveDetailed(server, slug, fetchImpl = globalThis.fetch.bind(globalThis)) {
    const url = `${server.replace(/\/$/, "")}/api/v1/remote/resolve/${encodeURIComponent(slug)}`;
    try {
      const res = await fetchImpl(url);
      let env;
      try {
        env = await res.json();
      } catch {
        return { online: false, summary: `\uC2DC\uADF8\uB110\uB9C1 \uC751\uB2F5 \uD30C\uC2F1 \uC2E4\uD328 (HTTP ${res.status}). url=${url}` };
      }
      if (env?.data?.online) return { online: true, summary: "online" };
      return {
        online: false,
        summary: `\uC2DC\uADF8\uB110\uB9C1 \uC11C\uBC84\uB294 \uC751\uB2F5\uD568 (HTTP ${res.status}). \uADF8\uB7EC\uB098 \uC774 PC(slug=${slug})\uAC00 online \uC73C\uB85C \uC811\uC18D\uB3FC \uC788\uC9C0 \uC54A\uC2B5\uB2C8\uB2E4 \u2014 PC \uC5D0\uC11C EZ-Folder(\uB9E4\uB2C8\uC800/\uD53C\uC5B4)\uAC00 \uC2E4\uD589 \uC911\uC778\uC9C0, \uAC19\uC740 slug \uC778\uC9C0 \uD655\uC778\uD558\uC138\uC694. url=${url}`
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return {
        online: false,
        summary: `\uC2DC\uADF8\uB110\uB9C1 \uC11C\uBC84\uC5D0 \uC5F0\uACB0\uD558\uC9C0 \uBABB\uD588\uC2B5\uB2C8\uB2E4 (\uB124\uD2B8\uC6CC\uD06C/\uC11C\uBC84 \uB2E4\uC6B4): ${msg} url=${url}`
      };
    }
  }
  /** ez-signaling 에서 connection/subscription JWT + 채널 발급. */
  async fetchToken() {
    const res = await this.fetchImpl(`${this.base}/token`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slug: this.slug, role: this.role, session: this.session, secret: this.secret ?? "" })
    });
    const env = await res.json();
    if (!env.ok || !env.data) throw new Error(env.error?.message ?? "token failed");
    return env.data;
  }
  /** 최초 1회 Centrifugo 연결 + 채널 구독. 이후 호출은 같은 promise 재사용. */
  ensureConnected() {
    if (this.connecting) return this.connecting;
    this.connecting = (async () => {
      const { token, subToken, channel } = await this.fetchToken();
      const wsUrl = this.server.replace(/^http/, "ws") + "/connection/websocket";
      const sseUrl = this.server + "/connection/sse";
      const cf = new Centrifuge(
        [
          { transport: "websocket", endpoint: wsUrl },
          { transport: "sse", endpoint: sseUrl }
        ],
        { token, getToken: async () => (await this.fetchToken()).token }
      );
      cf.on("connected", () => {
        this.connected = true;
      });
      cf.on("disconnected", () => {
        this.connected = false;
      });
      const sub = cf.newSubscription(channel, {
        token: subToken,
        getToken: async () => (await this.fetchToken()).subToken
      });
      sub.on("publication", (ctx) => {
        const d = ctx.data;
        if (!d || typeof d.type !== "string") return;
        this.queue.push({
          id: ++this.nextId,
          type: d.type,
          session: d.session ?? "",
          payload: d.payload,
          createAt: d.createAt ?? ""
        });
        const ws = this.waiters;
        this.waiters = [];
        for (const w of ws) w();
      });
      const subscribed = new Promise((resolve, reject) => {
        sub.on("subscribed", () => resolve());
        sub.on("error", (e) => reject(new Error("subscribe error: " + JSON.stringify(e))));
        setTimeout(() => reject(new Error("subscribe timeout")), 15e3);
      });
      this.cf = cf;
      this.sub = sub;
      sub.subscribe();
      cf.connect();
      await subscribed;
      this.connected = true;
    })();
    this.connecting.catch(() => {
      this.connecting = null;
    });
    return this.connecting;
  }
  /** peer 상주 호출 — Centrifugo 연결 보장(연결 자체가 presence). 무네트워크(이미 연결 시 no-op). */
  async heartbeat() {
    await this.ensureConnected();
  }
  /** 시그널 게시(offer/answer/ice). peer 발신은 X-Reg-Secret 필요. */
  async postSignal(type, payload, sessionOverride) {
    await this.ensureConnected();
    await this.fetchImpl(`${this.base}/signal`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({
        slug: this.slug,
        role: this.role,
        session: sessionOverride ?? this.session,
        type,
        payload
      })
    });
  }
  /** Centrifugo 가 받아둔 메시지를 비워 반환(무네트워크 드레인). 연결 보장 후. */
  async poll() {
    await this.ensureConnected();
    const q = this.queue;
    this.queue = [];
    return q;
  }
  /**
   * 이벤트 구동 드레인 — 큐에 있으면 즉시 반환, 없으면 다음 publication(또는 timeoutMs)까지 대기.
   * poll()+고정 delay 루프보다 수신 지연이 작다(Centrifugo push 를 폴링 간격 없이 즉시 받음).
   * 핸드셰이크(offer/answer/ice)의 왕복을 폴링 간격(250ms)만큼 단축한다.
   */
  async nextMessages(timeoutMs = 250) {
    await this.ensureConnected();
    if (this.queue.length === 0) {
      await new Promise((resolve) => {
        const w = () => {
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(() => {
          const i = this.waiters.indexOf(w);
          if (i >= 0) this.waiters.splice(i, 1);
          resolve();
        }, timeoutMs);
        this.waiters.push(w);
      });
    }
    const q = this.queue;
    this.queue = [];
    return q;
  }
  sessionId() {
    return this.session;
  }
  /** Centrifugo 연결 살아있는지(peer self-heal 판정). */
  isConnected() {
    return this.connected;
  }
  /** 내부 정리 — 구독/연결 끊고 상태 리셋(다음 ensureConnected 가 fresh 토큰으로 재구축 가능). */
  teardown() {
    const ws = this.waiters;
    this.waiters = [];
    for (const w of ws) w();
    try {
      this.sub?.unsubscribe();
    } catch {
    }
    try {
      this.cf?.disconnect();
    } catch {
    }
    this.cf = null;
    this.sub = null;
    this.connecting = null;
    this.connected = false;
  }
  /** Centrifugo 연결 종료(핸드셰이크 완료/피어 off 시). */
  disconnect() {
    this.teardown();
  }
  /**
   * 죽은 연결을 버리고 **fresh 토큰**으로 재구독(self-heal). 토큰 만료/절전 후 centrifuge-js 내부
   * 재연결이 stale 토큰을 물고 "token expired" 루프에 빠지면 — 통째 재생성이 유일한 탈출구다.
   * peer heartbeat 가 isConnected()=false 가 grace 동안 지속될 때 호출.
   */
  async forceReconnect() {
    this.teardown();
    await this.ensureConnected();
  }
  headers() {
    const h = { "Content-Type": "application/json" };
    if (this.role === "peer" && this.secret) h["X-Reg-Secret"] = this.secret;
    return h;
  }
};

// ../quasar/src/services/remote/protocol.ts
function encode(m) {
  return JSON.stringify(m);
}
function decode(s) {
  return JSON.parse(s);
}
var CHUNK_BYTES = 48 * 1024;
var RPC_CHUNK_BYTES = 32 * 1024;
function bytesToB64(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  if (typeof btoa === "function") return btoa(bin);
  return Buffer.from(bytes).toString("base64");
}
function b64ToBytes(b64) {
  if (typeof atob === "function") {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  return new Uint8Array(Buffer.from(b64, "base64"));
}

// ../quasar/src/services/remote/log.ts
var noopRemoteLog = {
  info() {
  },
  warn() {
  },
  error() {
  }
};
function consoleRemoteLog() {
  const fmt = (event, data) => `[remote][${event}]${data ? " " + JSON.stringify(data) : ""}`;
  return {
    info: (e, d) => console.info(fmt(e, d)),
    warn: (e, d) => console.warn(fmt(e, d)),
    error: (e, d) => console.error(fmt(e, d))
  };
}

// ../quasar/src/services/terminal/transport.ts
var LocalWsTransport = class {
  ws = null;
  dataCb = null;
  closeCb = null;
  openCb = null;
  constructor(baseUrl, token, opts) {
    const wsBase = baseUrl.replace(/^http/, "ws").replace(/\/$/, "");
    const q = new URLSearchParams({
      token,
      cwd: opts.cwd ?? "",
      cols: String(opts.cols),
      rows: String(opts.rows)
    });
    const ws = new WebSocket(`${wsBase}/v1/terminal/ws?${q.toString()}`);
    ws.binaryType = "arraybuffer";
    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") return;
      this.dataCb?.(new Uint8Array(ev.data));
    };
    ws.onopen = () => this.openCb?.();
    ws.onclose = () => this.closeCb?.();
    ws.onerror = () => this.closeCb?.();
    this.ws = ws;
  }
  onData(cb) {
    this.dataCb = cb;
  }
  onClose(cb) {
    this.closeCb = cb;
  }
  onOpen(cb) {
    this.openCb = cb;
  }
  send(bytes) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(bytes);
  }
  resize(cols, rows) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ t: "resize", cols, rows }));
    }
  }
  close() {
    try {
      this.ws?.close();
    } catch {
    }
    this.ws = null;
  }
};

// ../quasar/src/services/daemon/errors.ts
var DaemonNotReachableError = class extends Error {
  constructor(cause, hint) {
    super(
      hint ?? "ez-folder-daemon \uC5D0 \uC5F0\uACB0\uD560 \uC218 \uC5C6\uC2B5\uB2C8\uB2E4. \uBCC4\uB3C4 \uD130\uBBF8\uB110\uC5D0\uC11C `npm run daemon` \uC73C\uB85C daemon \uC744 \uC2E4\uD589\uD558\uC138\uC694."
    );
    this.name = "DaemonNotReachableError";
    this.cause = cause;
  }
};

// ../quasar/src/services/daemon/DaemonClient.ts
function resolveDaemonConfig() {
  const w = globalThis;
  if (w.__daemon && w.__daemon.baseUrl && w.__daemon.token) {
    return { baseUrl: w.__daemon.baseUrl, token: w.__daemon.token };
  }
  if (define_import_meta_env_default.DEV) {
    const base = define_import_meta_env_default.VITE_EZ_DAEMON_BASE;
    const tok = define_import_meta_env_default.VITE_EZ_DAEMON_TOKEN;
    if (base && tok) return { baseUrl: base, token: tok };
  }
  if (typeof localStorage !== "undefined") {
    const baseUrl = localStorage.getItem("ez-daemon.baseUrl") ?? "";
    const token = localStorage.getItem("ez-daemon.token") ?? "";
    if (baseUrl && token) return { baseUrl, token };
  }
  throw new DaemonNotReachableError(
    void 0,
    "daemon baseUrl/token \uBBF8\uC124\uC815. \uBCC4\uB3C4 \uD130\uBBF8\uB110\uC5D0\uC11C `npm run daemon` \uC73C\uB85C daemon \uC744 \uC2DC\uC791\uD55C \uB4A4 \uC790\uB3D9 \uC7AC\uBC1C\uACAC\uB429\uB2C8\uB2E4."
  );
}

// ../quasar/src/services/remote/PeerBridge.ts
function canonicalFsPath(p) {
  let s = p.replace(/\\/g, "/").toLowerCase();
  while (s.length > 1 && s.endsWith("/")) s = s.slice(0, -1);
  return s;
}
function hasDotDot(p) {
  return p.replace(/\\/g, "/").split("/").some((seg) => seg === "..");
}
function fsPathEq(a, b) {
  if (hasDotDot(a)) return false;
  return canonicalFsPath(a) === canonicalFsPath(b);
}
function fsPathUnder(child, parent) {
  if (hasDotDot(child)) return false;
  const c = canonicalFsPath(child);
  const p = canonicalFsPath(parent);
  if (c === p) return true;
  return c.startsWith(p + "/");
}
var ALLOW = [
  // ── liveness/auth probe (연결상태 표시줄 heartbeat) ──
  // 데스크톱 `/d` 는 devmode 앱 전체가 전역 daemon() 을 P2P 로 태운다. 그 연결상태 store 가
  // 2.5~15s 마다 connectionStatus() → GET /v1/readyz 로 살아있음+토큰유효를 판정한다.
  // 이게 화이트리스트에 없으면 거부(forbidden)→unreachable→footer "데몬 연결중" 영구 표시(INC-0140).
  { method: "GET", prefix: "/v1/readyz" },
  { method: "GET", prefix: "/v1/healthz" },
  // ── 관측성: 원격 클라이언트 로그를 PC frontend.log 로(원격 세션 디버깅). append-only 무해 ──
  { method: "POST", prefix: "/v1/logs/sink" },
  // ── 읽기: 라이선스 상태(LICENSE_GATE 표시 판정용 read-only; activate 등 변이는 차단 유지) ──
  { method: "GET", prefix: "/v1/license/state" },
  // ── 읽기: 파일시스템 탐색/조회 ──
  { method: "GET", prefix: "/v1/fs/drives" },
  { method: "GET", prefix: "/v1/fs/quick-access" },
  { method: "POST", prefix: "/v1/fs/browse" },
  { method: "POST", prefix: "/v1/fs/scan" },
  { method: "POST", prefix: "/v1/fs/stat" },
  { method: "POST", prefix: "/v1/fs/exists" },
  { method: "POST", prefix: "/v1/fs/read-text" },
  { method: "POST", prefix: "/v1/fs/read-bytes" },
  { method: "POST", prefix: "/v1/fs/zone-info" },
  // ── 읽기: 검색 ──
  { method: "POST", prefix: "/v1/search/keyword" },
  { method: "POST", prefix: "/v1/search/smart" },
  // ── 읽기: DB 문서/엑셀/청크 조회 (GET 만 — upsert/delete 는 별도) ──
  { method: "GET", prefix: "/v1/db/documents" },
  // 목록/상세(:id)/search/children/by-folder/...
  { method: "GET", prefix: "/v1/db/excel-rows" },
  // excel-rows + /sheets
  { method: "POST", prefix: "/v1/db/chunks/by-ids" },
  { method: "POST", prefix: "/v1/db/chunks/expand-parents" },
  // ── 읽기: 파일명 검색(Everything 식 fileScan) + 트리 브라우징 (전부 GET, 변이 아님) ──
  { method: "GET", prefix: "/v1/file-scan/status" },
  { method: "GET", prefix: "/v1/file-scan/search" },
  { method: "GET", prefix: "/v1/file-scan/files" },
  { method: "GET", prefix: "/v1/file-scan/dirs" },
  { method: "GET", prefix: "/v1/file-scan/skeleton" },
  { method: "GET", prefix: "/v1/file-scan/search-dirs" },
  { method: "GET", prefix: "/v1/file-scan/dir-files" },
  { method: "GET", prefix: "/v1/file-scan/dir-files-batch" },
  // ── 읽기: 미리보기 ──
  { method: "GET", prefix: "/v1/preview" },
  { method: "POST", prefix: "/v1/preview" },
  // ── 읽기: 문서 파싱/추출 (미리보기 텍스트) ──
  { method: "POST", prefix: "/v1/docs/parse" },
  { method: "POST", prefix: "/v1/docs/extract" },
  // ── 읽기: 인덱싱 상태(표시용, 변이 아님) ──
  { method: "GET", prefix: "/v1/indexing/status" },
  // ── 읽기: 설정 조회 (GET 만 — 변경은 차단) ──
  { method: "GET", prefix: "/v1/settings" },
  // settings + file-type-extensions + supported-extensions (GET)
  // ── 읽기: 워처 폴더 목록 (GET 만 — folders:add/remove POST 는 차단) ──
  { method: "GET", prefix: "/v1/watcher/folders" },
  // ── 쓰기(본인 계정): 파일 쓰기/이름변경 ──
  { method: "POST", prefix: "/v1/fs/write-text" },
  { method: "POST", prefix: "/v1/fs/write-bytes" },
  { method: "POST", prefix: "/v1/fs/rename" },
  // ── 쓰기(본인 계정): 문서 삭제 (인덱싱 유발 아님 — 정리) ──
  { method: "POST", prefix: "/v1/db/documents/delete" },
  { method: "POST", prefix: "/v1/db/documents/delete-files" },
  // ── 쓰기(본인 계정): 파일 공유 발급/해지. 막으려면 아래 3줄 제거(ADR-0158). ──
  { method: "GET", prefix: "/v1/share/list" },
  { method: "POST", prefix: "/v1/share/create" },
  { method: "POST", prefix: "/v1/share/revoke" }
];
var BACKPRESSURE_LIMIT = 4 * 1024 * 1024;
function defaultToken() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID().replace(/-/g, "");
  }
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}
var PeerBridge = class {
  sessions = /* @__PURE__ */ new Map();
  // token -> {exp, shareScope?}
  // 진행 중 업로드(드래그 IN) — id -> {저장경로, base64 청크 누적}. upload-end 에서 write-bytes.
  uploads = /* @__PURE__ */ new Map();
  fetchImpl;
  now;
  ttl;
  mkToken;
  chunkLen;
  log;
  allow;
  onSessionScope;
  terminalEnabled;
  terminalFactory;
  // 열린 원격 터미널 — term id → 로컬 PTY 전송.
  terminals = /* @__PURE__ */ new Map();
  constructor(opts = {}) {
    this.fetchImpl = opts.fetchImpl ?? globalThis.fetch.bind(globalThis);
    this.now = opts.now ?? (() => Date.now());
    this.ttl = opts.sessionTtlMs ?? 24 * 60 * 60 * 1e3;
    this.mkToken = opts.randomToken ?? defaultToken;
    this.chunkLen = opts.chunkLen ?? CHUNK_BYTES;
    this.log = opts.log ?? noopRemoteLog;
    this.allow = opts.allowRules ?? ALLOW;
    this.onSessionScope = opts.onSessionScope;
    this.terminalEnabled = opts.terminalEnabled ?? (() => true);
    this.terminalFactory = opts.terminalFactory ?? defaultTerminalFactory;
  }
  /** 새 P2P 채널에 핸들러 부착. */
  attach(ch) {
    ch.onMessage((text) => {
      void this.handle(ch, text);
    });
  }
  async handle(ch, text) {
    let msg;
    try {
      msg = decode(text);
    } catch {
      return;
    }
    if (msg.kind === "login") return this.login(ch, msg);
    if (msg.kind === "pair") return this.pair(ch, msg);
    if (msg.kind === "sync") return this.sync(ch, msg);
    if (msg.kind === "share-verify") return this.shareVerify(ch, msg);
    if (msg.kind === "rpc") return this.rpc(ch, msg);
    if (msg.kind === "download") return this.download(ch, msg);
    if (msg.kind === "upload-begin") return this.uploadBegin(ch, msg);
    if (msg.kind === "upload-chunk") return this.uploadChunk(msg);
    if (msg.kind === "upload-end") return this.uploadEnd(ch, msg);
    if (msg.kind === "term-open") return this.termOpen(ch, msg);
    if (msg.kind === "term-in") return this.termIn(msg);
    if (msg.kind === "term-resize") return this.termResize(msg);
    if (msg.kind === "term-close") return this.termClose(msg);
  }
  /** 유효 세션이면 그 값(만료 시 정리 후 undefined). 계정 세션 shareScope === undefined. */
  getSession(token) {
    if (!token) return void 0;
    const s = this.sessions.get(token);
    if (s === void 0) return void 0;
    if (this.now() > s.exp) {
      this.sessions.delete(token);
      return void 0;
    }
    return s;
  }
  validSession(token) {
    return this.getSession(token) !== void 0;
  }
  /**
   * 경로 정규화 — 화이트리스트 매처가 **fetch 가 실제로 보낼 경로**와 정확히 같은 것을 보게 한다.
   *
   * 보안 핵심(우회 RCE 봉쇄): `fetch()`(WHATWG URL)는 전송 전에 `..` 세그먼트를 접는다. 그래서
   * raw 경로로 화이트리스트를 매칭하면 matcher 와 실제 요청이 갈라진다 —
   * `POST /v1/fs/write-text/../open-terminal` 은 허용 prefix `/v1/fs/write-text/` 아래로 보여 통과하지만
   * fetch 는 `/v1/fs/open-terminal` 을 보낸다(PowerShell 스폰 → RCE). `../..` 로 임의 데몬 라우트 도달 가능.
   *
   * 처리:
   *  - 쿼리스트링 제거.
   *  - **방어적 거부(null)**: raw(정규화 전)에 `..` 세그먼트 / 백슬래시 / `//` / 제어문자가 있거나,
   *    `/v1/` 로 시작하지 않으면 거부. (정상 API 경로엔 이런 게 없다.)
   *  - `new URL(clean, 'http://x').pathname` 으로 정규화(fetch 자체 정규화 미러) 후에도 `/v1/` 로
   *    시작해야 하며, 아니면 거부.
   *
   * @returns 정규화된 pathname, 또는 거부 시 null.
   */
  canonicalPath(raw) {
    const clean = raw.split("?")[0] ?? raw;
    if (!clean.startsWith("/v1/")) return null;
    if (clean.includes("\\")) return null;
    if (clean.includes("//")) return null;
    {
      const low = clean.toLowerCase();
      if (low.includes("%2e") || low.includes("%2f") || low.includes("%5c") || low.includes("%00")) {
        return null;
      }
    }
    for (let i = 0; i < clean.length; i++) {
      if (clean.charCodeAt(i) < 32) return null;
    }
    if (clean.split("/").some((seg) => seg === "..")) return null;
    let normalized;
    try {
      normalized = new URL(clean, "http://x").pathname;
    } catch {
      return null;
    }
    if (!normalized.startsWith("/v1/")) return null;
    return normalized;
  }
  /**
   * 화이트리스트 매칭 — method + **경계(boundary)** 일치. **정규화된 경로**로만 매칭한다.
   *
   * `clean.startsWith(prefix)` 단순 prefix 매칭은 `/v1/fs/drives` 허용이 `/v1/fs/drives-evil` 을,
   * 혹은 짧은 prefix 가 더 긴 차단 경로를 인가하는 누수를 낸다. 경로 세그먼트 경계로만 매칭:
   *   path === prefix  또는  path 가 `prefix + '/'` 로 시작.
   * (prefix 는 슬래시 없는 정확 경로로 등록 — `folders:add` 처럼 콜론 액션 세그먼트도 method 가 다르면
   *  애초에 매칭되지 않는다.)
   *
   * canonicalPath(path) 가 null(위험/비정상 경로)이면 즉시 거부.
   */
  allowed(method, path) {
    const clean = this.canonicalPath(path);
    if (clean === null) return false;
    return this.allow.some(
      (a) => a.method === method && (clean === a.prefix || clean.startsWith(a.prefix + "/"))
    );
  }
  async login(ch, m) {
    let ok = false;
    try {
      const res = await this.fetchImpl("/v1/auth/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: m.username, password: m.password })
      });
      const j = await res.json().catch(() => ({ ok: false }));
      ok = res.ok && j.ok === true;
    } catch {
      ok = false;
    }
    if (ok) {
      const token = this.mkToken();
      const expiresAt = this.now() + this.ttl;
      this.sessions.set(token, { exp: expiresAt });
      ch.send(encode({ kind: "session", id: m.id, token, expiresAt }));
      this.log.info("login-ok", { session: ch.session });
    } else {
      ch.send(encode({ kind: "error", id: m.id, code: "unauthorized", message: "login failed" }));
      this.log.warn("login-fail", { session: ch.session });
    }
  }
  /**
   * 공유 검증 → **scoped 세션** 발급 (PLAN Phase 8, Task 8.2 — 보안 핵심).
   *
   * password 있으면 `/v1/share/verify {password}`, 없으면 `/v1/share/live {id}`. 데몬이
   * `{ok:true, share:{path,...}}` 면 fs/stat 로 isDir 판정 후 그 경로로 스코프된 토큰 발급.
   * miss/`{ok:false}` 면 토큰 미발급 + error(unauthorized).
   */
  /**
   * 페어링 토큰 제시 → 묶음 스냅샷 응답(ADR-0235 Phase 2). 로그인/세션 불요(토큰=1회용 인증).
   * 로컬 데몬 `/v1/group/redeem {token}` 으로 검증·소비 → 성공 시 스냅샷을 pair-result 로 전달.
   * 합류자는 이 스냅샷을 자기 데몬 `/v1/group/join` 에 적용한다(자격증명은 P2P 로만 흐름).
   */
  async pair(ch, m) {
    try {
      const res = await this.fetchImpl("/v1/group/redeem", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: m.token })
      });
      if (res.ok) {
        const snapshot = await res.json();
        ch.send(encode({ kind: "pair-result", id: m.id, ok: true, snapshot }));
        this.log.info("pair-ok", { session: ch.session });
        return;
      }
    } catch {
    }
    ch.send(encode({ kind: "pair-result", id: m.id, ok: false }));
    this.log.warn("pair-fail", { session: ch.session });
  }
  /**
   * PC↔PC 동기화(ADR-0235 Phase 3) — 원격 번들을 로컬 `/v1/group/sync` 로 머지(데몬이 group_key
   * 검증·충돌해소)하고 머지 후 로컬 번들을 sync-result 로 돌려준다. 인증은 번들의 group_key(데몬).
   */
  async sync(ch, m) {
    try {
      const res = await this.fetchImpl("/v1/group/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(m.bundle)
      });
      if (res.ok) {
        const bundle = await res.json();
        ch.send(encode({ kind: "sync-result", id: m.id, ok: true, bundle }));
        this.log.info("sync-ok", { session: ch.session });
        return;
      }
    } catch {
    }
    ch.send(encode({ kind: "sync-result", id: m.id, ok: false }));
    this.log.warn("sync-fail", { session: ch.session });
  }
  async shareVerify(ch, m) {
    let sharePath;
    let shareName = "";
    let shareFormat = "original";
    try {
      const url = m.password ? "/v1/share/verify" : "/v1/share/live";
      const body = m.password ? { password: m.password } : { id: m.shareId };
      const res = await this.fetchImpl(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const j = await res.json().catch(() => ({ ok: false }));
      if (res.ok && j.ok === true && j.share?.path) {
        sharePath = j.share.path;
        shareName = j.share.name ?? "";
        shareFormat = j.share.exportFormat ?? "original";
      }
    } catch {
      sharePath = void 0;
    }
    if (!sharePath) {
      ch.send(encode({ kind: "error", id: m.id, code: "unauthorized", message: "share verify failed" }));
      this.log.warn("share-verify-fail", { session: ch.session, shareId: m.shareId });
      return;
    }
    let isDir = false;
    try {
      const sres = await this.fetchImpl("/v1/fs/stat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: sharePath })
      });
      const sj = await sres.json().catch(() => ({}));
      isDir = sj.is_dir === true;
    } catch {
      isDir = false;
    }
    const token = this.mkToken();
    const expiresAt = this.now() + this.ttl;
    const scopePath = canonicalFsPath(sharePath);
    this.sessions.set(token, {
      exp: expiresAt,
      shareScope: { path: scopePath, isDir }
    });
    this.onSessionScope?.(ch.session, isDir ? scopePath : null);
    ch.send(
      encode({
        kind: "session",
        id: m.id,
        token,
        expiresAt,
        share: { path: sharePath, name: shareName, exportFormat: shareFormat, isDir }
      })
    );
    this.log.info("share-verify-ok", { session: ch.session, isDir });
  }
  /**
   * 공유 scoped 세션의 rpc 허용 판정 — 화이트리스트(allowed())를 **대체**한다.
   *
   * 허용:
   *  - read-text / preview*: body.path 가 scope 경계 안.
   *  - (폴더 공유만) fs/browse: body.path 가 scope.path 하위.
   * 그 외(전역 browse·search·모든 write·share/* 등) 전부 거부.
   */
  shareScopeAllowsRpc(scope, method, path, body) {
    const canon = this.canonicalPath(path);
    if (canon === null) return false;
    if (method !== "POST") return false;
    const bodyPath = body?.path;
    if (typeof bodyPath !== "string") return false;
    const inScope = (target) => scope.isDir ? fsPathUnder(target, scope.path) : fsPathEq(target, scope.path);
    if (canon === "/v1/fs/read-text") return inScope(bodyPath);
    if (canon === "/v1/preview" || canon.startsWith("/v1/preview/")) return inScope(bodyPath);
    if (scope.isDir && canon === "/v1/fs/browse") return fsPathUnder(bodyPath, scope.path);
    return false;
  }
  /** 공유 scoped 세션의 download(read-bytes) 허용 판정. */
  shareScopeAllowsDownload(scope, fsPath) {
    return scope.isDir ? fsPathUnder(fsPath, scope.path) : fsPathEq(fsPath, scope.path);
  }
  async rpc(ch, m) {
    const session = this.getSession(m.session);
    if (session === void 0) {
      this.log.warn("rpc-no-session", { path: m.path });
      ch.send(encode({ kind: "error", id: m.id, code: "unauthorized", message: "no session" }));
      return;
    }
    const permitted = session.shareScope ? this.shareScopeAllowsRpc(session.shareScope, m.method, m.path, m.body) : this.allowed(m.method, m.path);
    if (!permitted) {
      this.log.warn("rpc-forbidden", { method: m.method, path: m.path, scoped: !!session.shareScope });
      ch.send(encode({ kind: "error", id: m.id, code: "forbidden", message: "op not allowed" }));
      return;
    }
    const canonical = this.canonicalPath(m.path);
    if (canonical === null) {
      this.log.warn("rpc-forbidden", { method: m.method, path: m.path });
      ch.send(encode({ kind: "error", id: m.id, code: "forbidden", message: "op not allowed" }));
      return;
    }
    const qIdx = m.path.indexOf("?");
    const fetchUrl = qIdx >= 0 ? canonical + m.path.slice(qIdx) : canonical;
    try {
      const init = { method: m.method };
      if (m.body !== void 0) {
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify(m.body);
      }
      const res = await this.fetchImpl(fetchUrl, init);
      const body = await res.json().catch(() => null);
      if (res.status >= 400) this.log.warn("rpc-bad-status", { path: fetchUrl, status: res.status });
      await this.sendRpcResult(ch, m.id, res.status, body);
    } catch (e) {
      this.log.error("rpc-error", { path: canonical, err: shortErr(e) });
      ch.send(encode({ kind: "rpc-result", id: m.id, status: 0, body: { error: String(e) } }));
    }
  }
  /**
   * rpc 응답 전송 — body JSON 이 DataChannel 단일 메시지 한계를 넘으면 청크 분할.
   * 작으면 단일 rpc-result(기존 그대로). 크면(예: file-scan skeleton 12MB+) rpc-chunk
   * 여러 개 + rpc-end 로 보내 받는 쪽이 이어붙여 parse(INC — 큰 응답 전송 실패→채널 붕괴 수정).
   */
  async sendRpcResult(ch, id, status, body) {
    const json = JSON.stringify(body ?? null);
    if (json.length <= RPC_CHUNK_BYTES) {
      ch.send(encode({ kind: "rpc-result", id, status, body }));
      return;
    }
    let seq = 0;
    for (let off = 0; off < json.length; off += RPC_CHUNK_BYTES) {
      ch.send(encode({ kind: "rpc-chunk", id, seq: seq++, data: json.slice(off, off + RPC_CHUNK_BYTES) }));
      await this.applyBackpressure(ch);
    }
    ch.send(encode({ kind: "rpc-end", id, status, total: seq }));
    this.log.info("rpc-chunked", { bytes: json.length, chunks: seq });
  }
  /**
   * 백프레셔 — 송신 버퍼가 임계(BACKPRESSURE_LIMIT)를 넘으면 drain 될 때까지 대기.
   * 청크를 한꺼번에 쏟으면 DataChannel send 큐가 가득 차 send 가 throw 한다(12MB skeleton
   * 에서 'send queue is full' 발생, INC-0141). bufferedAmount/waitDrain 미구현 채널(테스트)은 no-op.
   */
  async applyBackpressure(ch) {
    if (ch.bufferedAmount && ch.waitDrain && ch.bufferedAmount() > BACKPRESSURE_LIMIT) {
      await ch.waitDrain();
    }
  }
  /** 다운로드/미리보기 — read-bytes 의 base64 를 청크로 스트리밍. */
  async download(ch, m) {
    const session = this.getSession(m.session);
    if (session === void 0) {
      ch.send(encode({ kind: "error", id: m.id, code: "unauthorized", message: "no session" }));
      return;
    }
    const permitted = session.shareScope ? this.shareScopeAllowsDownload(session.shareScope, m.path) : this.allowed("POST", "/v1/fs/read-bytes");
    if (!permitted) {
      this.log.warn("download-forbidden", { scoped: !!session.shareScope });
      ch.send(encode({ kind: "error", id: m.id, code: "forbidden", message: "op not allowed" }));
      return;
    }
    const file = baseName(m.path);
    try {
      const res = await this.fetchImpl("/v1/fs/read-bytes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: m.path })
      });
      const j = await res.json().catch(() => null);
      const b64 = j?.base64 ?? "";
      let seq = 0;
      for (let off = 0; off < b64.length; off += this.chunkLen) {
        ch.send(encode({ kind: "chunk", id: m.id, seq: seq++, b64: b64.slice(off, off + this.chunkLen) }));
        await this.applyBackpressure(ch);
      }
      ch.send(encode({ kind: "chunk-end", id: m.id, status: res.status, total: seq }));
      this.log.info("download", { file, status: res.status, chunks: seq });
    } catch (e) {
      this.log.error("download-error", { file, err: shortErr(e) });
      ch.send(encode({ kind: "error", id: m.id, code: "download_failed", message: String(e) }));
    }
  }
  /**
   * 업로드 시작(드래그 IN) — 세션·권한 검증 후 청크 누적 버퍼 등록. download 의 역방향.
   *
   * 게이트: 유효 세션 필수 + **공유 scoped 세션은 read-only → 거부**(계정 세션만 write).
   * 거부 시 error 를 보내고 버퍼를 등록하지 않는다(이후 chunk/end 는 무시 — 메모리 누적 방지).
   * write-bytes 경로(m.path)는 URL 이 아니라 body 의 {path} 라 canonicalPath 불요(download 와 동일).
   */
  uploadBegin(ch, m) {
    const session = this.getSession(m.session);
    if (session === void 0) {
      this.log.warn("upload-no-session", {});
      ch.send(encode({ kind: "error", id: m.id, code: "unauthorized", message: "no session" }));
      return;
    }
    if (session.shareScope || !this.allowed("POST", "/v1/fs/write-bytes")) {
      this.log.warn("upload-forbidden", { scoped: !!session.shareScope });
      ch.send(encode({ kind: "error", id: m.id, code: "forbidden", message: "op not allowed" }));
      return;
    }
    this.uploads.set(m.id, { path: m.path, chunks: [] });
  }
  /** 업로드 청크 누적 — begin 에서 거부된(미등록) id 는 무시. */
  uploadChunk(m) {
    const u = this.uploads.get(m.id);
    if (u) u.chunks[m.seq] = m.b64;
  }
  /** 업로드 종료 — base64 재조립 후 write-bytes 로 저장하고 rpc-result(같은 id)로 응답. */
  async uploadEnd(ch, m) {
    const u = this.uploads.get(m.id);
    if (u === void 0) return;
    this.uploads.delete(m.id);
    const base64 = u.chunks.join("");
    const file = baseName(u.path);
    try {
      const res = await this.fetchImpl("/v1/fs/write-bytes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: u.path, base64 })
      });
      const body = await res.json().catch(() => null);
      if (res.status >= 400) this.log.warn("upload-bad-status", { file, status: res.status });
      else this.log.info("upload", { file, status: res.status, chunks: u.chunks.length });
      ch.send(encode({ kind: "rpc-result", id: m.id, status: res.status, body }));
    } catch (e) {
      this.log.error("upload-error", { file, err: shortErr(e) });
      ch.send(encode({ kind: "rpc-result", id: m.id, status: 0, body: { error: String(e) } }));
    }
  }
  /**
   * 원격 터미널 열기 — **게이트**: 유효 계정 세션 + scoped(공유) 세션 아님 + 설정 허용.
   * 통과 시 로컬 데몬 PTY(WS)를 열어 출력을 term-data 로, 종료를 term-exit 로 폰에 중계.
   * 거부 시 term-exit(reason) 만 보낸다(셸 안 염).
   */
  termOpen(ch, m) {
    const session = this.getSession(m.session);
    if (session === void 0) {
      ch.send(encode({ kind: "term-exit", id: m.id, reason: "unauthorized" }));
      return;
    }
    if (session.shareScope) {
      this.log.warn("term-forbidden", { reason: "scoped" });
      ch.send(encode({ kind: "term-exit", id: m.id, reason: "forbidden: shared session" }));
      return;
    }
    if (!this.terminalEnabled()) {
      this.log.warn("term-forbidden", { reason: "disabled" });
      ch.send(encode({ kind: "term-exit", id: m.id, reason: "remote terminal disabled" }));
      return;
    }
    let transport;
    try {
      transport = this.terminalFactory({ cwd: m.cwd ?? "", cols: m.cols, rows: m.rows });
    } catch (e) {
      this.log.error("term-open-fail", { err: shortErr(e) });
      ch.send(encode({ kind: "term-exit", id: m.id, reason: shortErr(e) }));
      return;
    }
    this.terminals.set(m.id, transport);
    transport.onData((bytes) => ch.send(encode({ kind: "term-data", id: m.id, b64: bytesToB64(bytes) })));
    transport.onClose(() => {
      this.terminals.delete(m.id);
      ch.send(encode({ kind: "term-exit", id: m.id }));
    });
    this.log.info("term-open", { id: m.id });
  }
  termIn(m) {
    this.terminals.get(m.id)?.send(b64ToBytes(m.b64));
  }
  termResize(m) {
    this.terminals.get(m.id)?.resize(m.cols, m.rows);
  }
  termClose(m) {
    const t = this.terminals.get(m.id);
    if (t) {
      t.close();
      this.terminals.delete(m.id);
    }
  }
};
function defaultTerminalFactory(opts) {
  const cfg = resolveDaemonConfig();
  return new LocalWsTransport(cfg.baseUrl, cfg.token, opts);
}
function shortErr(e) {
  const s = e instanceof Error ? e.message : String(e);
  return s.length > 120 ? s.slice(0, 120) + "\u2026" : s;
}
function baseName(p) {
  const parts = p.split(/[\\/]/);
  return parts[parts.length - 1] || p;
}

// ../quasar/src/services/remote/RemoteTransport.ts
var DEFAULT_RTC = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
  // 후보를 미리 모아둬(연결 생성 즉시 gathering 시작) offer/answer 시점에 host 후보가 준비되게 한다.
  iceCandidatePoolSize: 4
};
var defaultFactory = (config) => new RTCPeerConnection(config);
var BUFFERED_LOW = 1 * 1024 * 1024;
function wrapChannel(dc, pc, session) {
  const msgCbs = [];
  const closeCbs = [];
  dc.onmessage = (ev) => {
    const data = typeof ev.data === "string" ? ev.data : "";
    for (const cb of msgCbs) cb(data);
  };
  dc.onclose = () => {
    for (const cb of closeCbs) cb();
  };
  try {
    dc.bufferedAmountLowThreshold = BUFFERED_LOW;
  } catch {
  }
  return {
    session,
    send: (text) => dc.send(text),
    bufferedAmount: () => dc.bufferedAmount,
    waitDrain: () => new Promise((resolve) => {
      if (dc.bufferedAmount <= BUFFERED_LOW) {
        resolve();
        return;
      }
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        dc.removeEventListener("bufferedamountlow", onLow);
        clearInterval(poll);
        resolve();
      };
      const onLow = () => finish();
      dc.addEventListener("bufferedamountlow", onLow);
      const poll = setInterval(() => {
        if (dc.bufferedAmount <= BUFFERED_LOW) finish();
      }, 50);
    }),
    onMessage: (cb) => msgCbs.push(cb),
    onClose: (cb) => closeCbs.push(cb),
    close: () => {
      try {
        dc.close();
      } catch {
      }
      pc.close();
    }
  };
}
function startPeer(signaling, onConnection, opts = {}) {
  const log2 = opts.log ?? noopRemoteLog;
  const factory = opts.pcFactory ?? defaultFactory;
  const conns = /* @__PURE__ */ new Map();
  let running = true;
  let netUp;
  const STALE_LIMIT = 2;
  let staleBeats = 0;
  let healing = false;
  const beat = async () => {
    try {
      await signaling.heartbeat();
      if (signaling.isConnected()) {
        staleBeats = 0;
        if (netUp === false) log2.info("signaling-up");
        netUp = true;
        return;
      }
      staleBeats += 1;
      if (netUp !== false) log2.warn("signaling-stale", { beats: staleBeats });
      netUp = false;
    } catch (e) {
      staleBeats += 1;
      if (netUp !== false) log2.warn("heartbeat-fail", { err: shortErr2(e), beats: staleBeats });
      netUp = false;
    }
    if (staleBeats >= STALE_LIMIT && !healing) {
      healing = true;
      log2.warn("signaling-rebuild", { reason: "stale-connection", beats: staleBeats });
      try {
        await signaling.forceReconnect();
        staleBeats = 0;
        netUp = true;
        log2.info("signaling-rebuilt");
      } catch (e) {
        log2.warn("signaling-rebuild-fail", { err: shortErr2(e) });
      } finally {
        healing = false;
      }
    }
  };
  void beat();
  const hb = setInterval(() => void beat(), opts.heartbeatMs ?? 15e3);
  const interval = opts.pollIntervalMs ?? 250;
  void (async () => {
    while (running) {
      const msgs = await signaling.nextMessages(interval).catch(() => []);
      for (const m of msgs) {
        const session = m.session;
        if (!session) continue;
        if (m.type === "offer" && !conns.has(session)) {
          await handleOffer(session, m.payload);
        } else if (m.type === "ice") {
          const pc = conns.get(session);
          if (pc) {
            const cand = m.payload;
            log2.info("ice-remote-cand", { session, type: candType(cand.candidate ?? ""), proto: candProto(cand.candidate ?? "") });
            await pc.addIceCandidate(cand).catch((e) => log2.warn("ice-remote-add-fail", { session, err: shortErr2(e) }));
          }
        }
      }
    }
  })();
  async function handleOffer(session, sdp) {
    log2.info("peer-offer-recv", { session });
    const pc = factory(opts.pcConfig ?? DEFAULT_RTC);
    conns.set(session, pc);
    let localSrflx = 0;
    let localHost = 0;
    let localRelay = 0;
    pc.addEventListener?.("connectionstatechange", () => {
      const s = pc.connectionState;
      log2.info("ice-conn-state", { session, conn: s, ice: pc.iceConnectionState });
      if (s === "failed") log2.error("peer-conn-failed", { session, ice: pc.iceConnectionState });
    });
    pc.addEventListener?.("iceconnectionstatechange", () => {
      log2.info("ice-state", { session, ice: pc.iceConnectionState });
    });
    pc.addEventListener?.("icegatheringstatechange", () => {
      if (pc.iceGatheringState === "complete") {
        log2.info("ice-gather-done", { session, host: localHost, srflx: localSrflx, relay: localRelay });
        if (localSrflx === 0 && localRelay === 0) {
          log2.warn("ice-no-public-candidate", { session, hint: "STUN \uC2E4\uD328/\uCC28\uB2E8 \u2014 \uC678\uBD80\uB9DD\uC5D0\uC11C \uC9C1\uACB0 \uBD88\uAC00(WSL2 NAT?). TURN \uD544\uC694" });
        }
      }
    });
    pc.onicecandidate = (ev) => {
      if (ev.candidate) {
        const t = candType(ev.candidate.candidate);
        if (t === "srflx") localSrflx += 1;
        else if (t === "relay") localRelay += 1;
        else if (t === "host") localHost += 1;
        log2.info("ice-local-cand", { session, type: t, proto: candProto(ev.candidate.candidate) });
        void signaling.postSignal("ice", ev.candidate.toJSON(), session).catch(() => {
        });
      }
    };
    pc.ondatachannel = (ev) => {
      const ch = wrapChannel(ev.channel, pc, session);
      ch.onClose(() => {
        conns.delete(session);
        log2.info("peer-conn-closed", { session });
      });
      log2.info("peer-conn-open", { session });
      onConnection(ch);
    };
    try {
      await pc.setRemoteDescription(sdp);
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      await signaling.postSignal("answer", pc.localDescription, session);
      log2.info("peer-answer-sent", { session });
    } catch (e) {
      log2.error("peer-answer-fail", { session, err: shortErr2(e) });
      conns.delete(session);
    }
  }
  return {
    stop: () => {
      running = false;
      clearInterval(hb);
      for (const pc of conns.values()) pc.close();
      conns.clear();
      signaling.disconnect?.();
    }
  };
}
function candType(c) {
  const m = / typ (\w+)/.exec(c);
  return m?.[1] ?? "?";
}
function candProto(c) {
  const m = /^candidate:\S+ \d+ (\w+)/i.exec(c) ?? / (udp|tcp) /i.exec(c);
  return m?.[1]?.toLowerCase() ?? "?";
}
function shortErr2(e) {
  const s = e instanceof Error ? e.message : String(e);
  return s.length > 120 ? s.slice(0, 120) + "\u2026" : s;
}

// src/peer.ts
var G = globalThis;
if (!G.WebSocket) G.WebSocket = WS;
var P = ndc;
if (!G.RTCPeerConnection) G.RTCPeerConnection = P.RTCPeerConnection;
if (!G.RTCSessionDescription) G.RTCSessionDescription = P.RTCSessionDescription;
if (!G.RTCIceCandidate) G.RTCIceCandidate = P.RTCIceCandidate;
if (!G.RTCDataChannel && P.RTCDataChannel) G.RTCDataChannel = P.RTCDataChannel;
var SERVER = (process.env.EZ_REMOTE_SERVER || "https://ez-folder.bbo-odd.com").replace(/\/$/, "");
var DAEMON_BASE = (process.env.EZFD_DAEMON_BASE || "http://127.0.0.1:59100").replace(/\/$/, "");
var DAEMON_TOKEN = process.env.EZFD_DAEMON_TOKEN || "";
var log = consoleRemoteLog();
function daemonFetch(input, init) {
  const url = input.startsWith("/") ? DAEMON_BASE + input : input;
  const headers = new Headers(init?.headers);
  if (DAEMON_TOKEN && !headers.has("Authorization")) headers.set("Authorization", `Bearer ${DAEMON_TOKEN}`);
  return fetch(url, { ...init, headers });
}
async function waitForAccount() {
  let warned = false;
  for (; ; ) {
    try {
      const res = await daemonFetch("/v1/auth/account");
      if (res.ok) {
        const j = await res.json();
        if (typeof j.username === "string" && j.username.length > 0) {
          log.info("account-ready", { username: j.username });
          return;
        }
      }
    } catch {
    }
    if (!warned) {
      log.info("await-account", { msg: "\uACC4\uC815 \uC5C6\uC74C \u2014 online \uBCF4\uB958(\uC2AC\uB7EC\uADF8 \uBBF8\uB4F1\uB85D). \uACC4\uC815 \uC0DD\uC131 \uC2DC \uC790\uB3D9 \uC9C4\uC785" });
      warned = true;
    }
    await new Promise((r) => setTimeout(r, 5e3));
  }
}
async function main() {
  if (!DAEMON_TOKEN) log.warn("boot", { msg: "EZFD_DAEMON_TOKEN \uBBF8\uC124\uC815 \u2014 \uB370\uBAAC \uC778\uC99D \uC2E4\uD328 \uAC00\uB2A5" });
  const mcRes = await daemonFetch("/v1/license/machine-code");
  const mc = await mcRes.json();
  const deviceId = mc.machineCode;
  if (!deviceId) throw new Error(`machine-code \uC870\uD68C \uC2E4\uD328 (status=${mcRes.status}) \u2014 EZFD_DAEMON_BASE/TOKEN \uD655\uC778`);
  log.info("device", { deviceId });
  await waitForAccount();
  const reg = await SignalingClient.register(SERVER, deviceId);
  log.info("register", { slug: reg.slug });
  const signaling = new SignalingClient({
    server: SERVER,
    slug: reg.slug,
    session: "",
    role: "peer",
    secret: reg.registrationSecret
  });
  const bridge = new PeerBridge({ log, fetchImpl: daemonFetch, terminalEnabled: () => false });
  const handle = startPeer(signaling, (ch) => bridge.attach(ch), { log });
  const desktopUrl = `${SERVER}/d/${reg.slug}`;
  const mobileUrl = `${SERVER}/m/${reg.slug}`;
  log.info("peer-online", { slug: reg.slug, desktopUrl, mobileUrl });
  console.log(`
[ez-folder-peer] ONLINE
  desktop: ${desktopUrl}
  mobile : ${mobileUrl}
`);
  const stop = () => {
    try {
      handle.stop();
    } catch {
    }
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  setInterval(() => {
  }, 1 << 30);
}
main().catch((e) => {
  console.error("[ez-folder-peer] FATAL", e instanceof Error ? e.stack : e);
  process.exit(1);
});
