// PROTOTYPE, throwaway. Spike for https://github.com/galer7/draw-out/issues/2
// No dependencies on purpose: the MCP server and the proxy are plain node:http.
const vscode = require("vscode");
const http = require("node:http");
const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { execFile } = require("node:child_process");

const STATE_DIR = path.join(os.homedir(), ".draw-out");
const LOG_FILE = path.join(STATE_DIR, "spike.log");
const T3_APP = "/Applications/T3 Code (Alpha).app";

let output;
function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  output?.appendLine(line);
  try {
    fs.appendFileSync(LOG_FILE, line + "\n");
  } catch {}
}

const cfg = () => vscode.workspace.getConfiguration("drawOut");
const t3BaseDir = () => cfg().get("t3BaseDir").replace(/^~/, os.homedir());
// One bearer token per T3 server, named by its port: ~/.draw-out/t3-token-3773.
const tokenFile = () => path.join(STATE_DIR, `t3-token-${new URL(cfg().get("t3Url")).port}`);
// A patched T3 reads this folder and attaches our MCP server to chats in our workspace folders.
let bridgeFile;

// ---------- T3 ----------

function t3Cli(args) {
  // drawOut.t3Cli runs another T3 build, e.g. ["node", "/path/to/t3code/apps/server/src/bin.ts"].
  const [cmd, ...pre] = cfg().get("t3Cli").length
    ? cfg().get("t3Cli")
    : [`${T3_APP}/Contents/MacOS/T3 Code (Alpha)`, `${T3_APP}/Contents/Resources/app.asar/apps/server/dist/bin.mjs`];
  return new Promise((resolve, reject) => {
    execFile(
      cmd,
      [...pre, ...args, "--base-dir", t3BaseDir()],
      { env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" } },
      (err, stdout, stderr) => (err ? reject(new Error(stderr || err.message)) : resolve(stdout)),
    );
  });
}

// T3 orchestration v2 rejects reads without this header. A v1 server ignores it.
async function t3Fetch(pathname, init = {}) {
  const token = fs.readFileSync(tokenFile(), "utf8").trim();
  const res = await fetch(cfg().get("t3Url") + pathname, {
    ...init,
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      "x-t3-orchestration-protocol": "2",
      ...init.headers,
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${pathname}: ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : null;
}

// Both versions list threads with id, title, runtimeMode and interactionMode. Only v2 has schemaVersion.
const t3Shell = () => t3Fetch("/api/orchestration/shell");
const isV2 = (shell) => "schemaVersion" in shell;

// v2 has no HTTP dispatch: commands go over the /ws Effect RPC socket, JSON, one message per frame.
// One request per socket: send a Request frame, wait for its Exit frame.
async function t3Rpc(tag, payload) {
  const { ticket } = await t3Fetch("/api/auth/websocket-ticket", { method: "POST" });
  const url = new URL("/ws", cfg().get("t3Url").replace(/^http/, "ws"));
  url.searchParams.set("wsTicket", ticket);
  url.searchParams.set("orchestrationProtocol", "2");
  const ws = new WebSocket(url);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => done(reject, new Error(`${tag}: timeout`)), 15000);
    const done = (fn, v) => (clearTimeout(timer), ws.close(), fn(v));
    ws.onopen = () => ws.send(JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] }));
    ws.onerror = (e) => done(reject, new Error(`${tag}: ${e.message ?? "socket error"}`));
    ws.onclose = (e) => done(reject, new Error(`${tag}: socket closed ${e.code} ${e.reason}`));
    ws.onmessage = (e) => {
      for (const m of [].concat(JSON.parse(e.data))) {
        if (m._tag === "Defect") return done(reject, new Error(`${tag}: ${JSON.stringify(m.defect).slice(0, 300)}`));
        if (m._tag !== "Exit" || String(m.requestId) !== "1") continue;
        if (m.exit._tag === "Success") return done(resolve, m.exit.value);
        return done(reject, new Error(`${tag}: ${JSON.stringify(m.exit.cause).slice(0, 300)}`));
      }
    };
  });
}

// The T3 thread that called a tool. Stock v1 T3 does not tell an MCP server which thread calls it,
// so look for the running thread with an unfinished call to this tool in its activity log.
// The patched v2 T3 sends X-T3-Thread-Id, so this guess is v1 only.
async function findCallingThread(toolName, args) {
  const shell = await t3Shell();
  if (isV2(shell)) return null;
  const running = shell.threads.filter((t) => t.session?.activeTurnId);
  // Two threads can call the same tool at once: then match the call's arguments too.
  const argsSnippet = JSON.stringify(args.body ?? "").slice(1, 40);
  for (let attempt = 0; attempt < 5; attempt++) {
    const callers = [];
    for (const t of running) {
      const { activities = [] } = (await t3Fetch(`/api/orchestration/threads/${t.id}`)).thread;
      const done = new Set(activities.filter((a) => a.kind === "tool.completed").map((a) => a.payload?.toolCallId));
      const openIds = activities
        .filter((a) => a.kind === "tool.started" && a.payload?.data?.toolName === `mcp__draw-out__${toolName}`)
        .map((a) => a.payload.toolCallId)
        .filter((id) => !done.has(id));
      if (!openIds.length) continue;
      const argsMatch = activities.some(
        (a) => openIds.includes(a.payload?.toolCallId) && String(a.payload?.detail ?? "").includes(argsSnippet),
      );
      callers.push({ t, argsMatch });
    }
    const matched = callers.filter((c) => c.argsMatch);
    if (callers.length === 1) return callers[0].t;
    if (matched.length === 1) return matched[0].t;
    log(`findCallingThread attempt ${attempt}: ${callers.length} candidates, ${matched.length} match args, ${running.length} running`);
    await new Promise((r) => setTimeout(r, 300));
  }
  return null;
}

async function sendToThread(thread, text) {
  if (thread.v2) {
    // deliveryIntent "auto" lets the server pick: start now, steer the active run, or queue after it.
    return t3Rpc("orchestration.dispatchCommand", {
      type: "message.dispatch",
      commandId: crypto.randomUUID(),
      createdBy: "user",
      creationSource: "web",
      threadId: thread.id,
      messageId: crypto.randomUUID(),
      text,
      attachments: [],
      deliveryIntent: "auto",
      dispatchMode: { type: "start_immediately" },
    });
  }
  return t3Fetch("/api/orchestration/dispatch", {
    method: "POST",
    body: JSON.stringify({
      type: "thread.turn.start",
      commandId: crypto.randomUUID(),
      threadId: thread.id,
      message: { messageId: crypto.randomUUID(), role: "user", text, attachments: [] },
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
      createdAt: new Date().toISOString(),
    }),
  });
}

// ---------- Chat panel ----------

let proxyServer;

// Runs inside the framed T3 page. In the panel, the browser fires no paste event in this page and denies it
// the clipboard, but the extension can read and write the clipboard. So ⌘V asks the extension for the text,
// and ⌘C hands it the selection: page -> panel page -> extension, and back. Each step logs to spike.log.
const INJECT_JS = `(() => {
  const log = (m) => fetch("/__drawout/log?m=" + encodeURIComponent(m)).catch(() => {});
  let lastPaste = 0;
  document.addEventListener("paste", () => { lastPaste = Date.now(); log("native paste"); }, true);
  window.addEventListener("message", (e) => {
    if (e.source !== parent || !e.data || e.data.type !== "drawout-paste") return;
    log("paste ok=" + document.execCommand("insertText", false, e.data.text));
  });
  document.addEventListener("keydown", (e) => {
    if (!(e.metaKey || e.ctrlKey) || e.altKey || e.shiftKey) return;
    const key = e.key.toLowerCase();
    if (key === "c" || key === "x") {
      const text = String(getSelection());
      if (text) parent.postMessage({ type: "drawout-copy", text }, "*");
      return;
    }
    if (key !== "v") return;
    const at = Date.now();
    setTimeout(() => { if (lastPaste < at) parent.postMessage({ type: "drawout-paste-request" }, "*"); }, 80);
  }, true);
  let lastPath = "";
  setInterval(() => {
    if (location.pathname === lastPath) return;
    lastPath = location.pathname;
    parent.postMessage({ type: "drawout-location", path: lastPath }, "*");
  }, 500);
  log("inject loaded");
})();`;
async function startProxy(rewriteCookies) {
  proxyServer?.close();
  const target = new URL(cfg().get("t3Url"));
  const fixCookie = (c) =>
    rewriteCookies ? c.replace(/;\s*SameSite=Lax/i, "") + "; SameSite=None; Secure" : c;

  proxyServer = http.createServer((req, res) => {
    if (req.url === "/__drawout/inject.js") {
      return res.writeHead(200, { "content-type": "text/javascript", "cache-control": "no-store" }).end(INJECT_JS);
    }
    if (req.url.startsWith("/__drawout/log?")) {
      log(`panel ${new URL(req.url, "http://x").searchParams.get("m")}`);
      return res.writeHead(204).end();
    }
    const hasCookie = /t3_session_/.test(req.headers.cookie || "");
    const upstream = http.request(
      {
        host: target.hostname,
        port: target.port,
        path: req.url,
        method: req.method,
        // Uncompressed, so the HTML page can take the injected script.
        headers: { ...req.headers, host: target.host, "accept-encoding": "identity" },
      },
      (up) => {
        const headers = { ...up.headers };
        if (headers["set-cookie"]) headers["set-cookie"] = headers["set-cookie"].map(fixCookie);
        if (req.url.startsWith("/api/auth") || req.url === "/" || req.url.startsWith("/pair"))
          log(`proxy ${req.method} ${req.url} cookie=${hasCookie} -> ${up.statusCode}${headers["set-cookie"] ? " set-cookie" : ""}`);
        if (!String(headers["content-type"]).startsWith("text/html")) {
          res.writeHead(up.statusCode, headers);
          return up.pipe(res);
        }
        let html = "";
        up.setEncoding("utf8");
        up.on("data", (c) => (html += c));
        up.on("end", () => {
          delete headers["content-length"];
          delete headers.etag;
          res.writeHead(up.statusCode, headers);
          res.end(html.replace(/<head[^>]*>/i, (m) => `${m}<script src="/__drawout/inject.js"></script>`));
        });
      },
    );
    upstream.on("error", (e) => {
      log(`proxy error ${e.message}`);
      res.writeHead(502).end();
    });
    req.pipe(upstream);
  });

  proxyServer.on("upgrade", (req, socket, head) => {
    log(`proxy WS ${req.url.split("?")[0]} cookie=${/t3_session_/.test(req.headers.cookie || "")}`);
    const up = net.connect(Number(target.port), target.hostname, () => {
      const lines = [`${req.method} ${req.url} HTTP/1.1`];
      for (let i = 0; i < req.rawHeaders.length; i += 2) {
        const [k, v] = [req.rawHeaders[i], req.rawHeaders[i + 1]];
        lines.push(`${k}: ${k.toLowerCase() === "host" ? target.host : v}`);
      }
      up.write(lines.join("\r\n") + "\r\n\r\n");
      if (head?.length) up.write(head);
      up.pipe(socket);
      socket.pipe(up);
    });
    up.on("error", () => socket.destroy());
    socket.on("error", () => up.destroy());
  });

  await new Promise((r) => proxyServer.listen(cfg().get("proxyPort"), "127.0.0.1", r));
  const { port } = proxyServer.address();
  log(`proxy on ${port} -> ${target.host} rewrite=${rewriteCookies}`);
  return `http://127.0.0.1:${port}`;
}

// The T3 project for this window's first folder. Adds it when missing, with chats in the folder itself:
// a chat in a T3 worktree runs in a folder no editor window has open, so it would get no editor tools.
async function workspaceProject() {
  const root = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!root) return null;
  const shell = await t3Shell();
  const found = shell.projects.find((p) => p.workspaceRoot === root);
  if (found) return found.id;
  if (!isV2(shell)) return null;
  const projectId = crypto.randomUUID();
  const mutate = (body) => t3Fetch("/api/projects/mutate", { method: "POST", body: JSON.stringify({ commandId: crypto.randomUUID(), projectId, ...body }) });
  await mutate({ type: "project.create", title: path.basename(root), workspaceRoot: root });
  await mutate({ type: "project.update", defaultThreadEnvMode: "local" });
  log(`added project ${path.basename(root)} ${projectId}`);
  return projectId;
}

// The chat lives in a webview view in the secondary sidebar, so code always opens in the editor area.
let chatView;
const chatViewProvider = {
  resolveWebviewView(view) {
    chatView = view;
    view.webview.options = { enableScripts: true };
    view.webview.onDidReceiveMessage(async (m) => {
      if (m.type === "drawout-copy") await vscode.env.clipboard.writeText(m.text);
      // A chat's address is /<environmentId>/<threadId>.
      if (m.type === "drawout-location") setActiveThread(m.path.split("/").filter(Boolean)[1] ?? null);
      if (m.type === "drawout-paste-request") {
        view.webview.postMessage({ type: "drawout-paste", text: await vscode.env.clipboard.readText() });
      }
    });
    loadChat(view.webview);
  },
};

async function openChat(modeOverride) {
  if (chatView && typeof modeOverride === "string") await loadChat(chatView.webview, modeOverride);
  await vscode.commands.executeCommand("drawOut.chat.focus");
}

async function loadChat(webview, modeOverride) {
  const mode = typeof modeOverride === "string" ? modeOverride : cfg().get("chatMode");
  const base = mode === "direct" ? cfg().get("t3Url") : await startProxy(mode === "proxy-samesite-none");
  let query = "";
  try {
    const projectId = await workspaceProject();
    if (projectId) query = `?project=${projectId}`;
  } catch (e) {
    log(`workspace project failed: ${e.message}`);
  }
  let src = `${base}/${query}`;
  try {
    const { credential } = JSON.parse(await t3Cli(["auth", "pairing", "create", "--ttl", "5m", "--label", "draw-out-panel", "--json"]));
    src = `${base}/pair${query}#token=${credential}`;
  } catch (e) {
    log(`pairing failed: ${e.message}`);
  }
  log(`openChat mode=${mode} base=${base}${query}`);

  const nonce = crypto.randomUUID();
  webview.html = `<!doctype html><html><head>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src http://127.0.0.1:* http://localhost:*; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
<style>html,body,iframe{margin:0;padding:0;border:0;width:100%;height:100vh;overflow:hidden}</style>
</head><body><iframe src="${src}" allow="clipboard-read; clipboard-write"></iframe>
<script nonce="${nonce}">
  const vscode = acquireVsCodeApi();
  const frame = document.querySelector("iframe");
  window.addEventListener("message", (e) => {
    if (e.source === frame.contentWindow && e.data?.type?.startsWith("drawout-")) vscode.postMessage(e.data);
    else if (e.data?.type === "drawout-paste") frame.contentWindow.postMessage(e.data, "*");
  });
</script></body></html>`;
}

// ---------- Traces ----------
//
// A trace belongs to one T3 thread (a chat) and holds ordered steps. All traces live in VS Code's storage for
// this folder, so a reload or a restart keeps them. The notes of one trace at a time sit on the code: the
// "shown" trace. The agent deletes a trace softly: it moves to "Deleted", and only Gabriel empties that.
//
// store = {
//   traces: { [traceId]: { id, title, thread: { id, title, v2, runtimeMode, interactionMode }, createdAt, deletedAt, steps: [stepId] } },
//   steps:  { [stepId]: { id, traceId, uri, startLine, endLine, body, replies: [{ author, text }] } },  // lines 0-based
//   current: { [threadId]: traceId },  // the trace the agent adds to, per chat
//   shown: traceId | null,
// }

const STORE_KEY = "drawOut.traces.v1";
let comments;
let workspaceState;
let store = { traces: {}, steps: {}, current: {}, shown: null };
let activeThreadId = null; // the chat open in the sidebar, from the T3 page's address
let currentStep = null;
const shownThreads = new Map(); // stepId -> CommentThread, for the shown trace only
const lastStepAt = new Map(); // traceId -> time of its last new step

const newId = (prefix) => `${prefix}-${crypto.randomUUID().slice(0, 6)}`;
const save = () => workspaceState.update(STORE_KEY, store);
const liveTraces = (threadId) =>
  Object.values(store.traces).filter((t) => t.thread.id === threadId && !t.deletedAt);

function loadStore(state) {
  workspaceState = state;
  store = { traces: {}, steps: {}, current: {}, shown: null, ...state.get(STORE_KEY) };
}

function comment(body, author) {
  return { body: new vscode.MarkdownString(body), mode: vscode.CommentMode.Preview, author: { name: author } };
}

// A step's short title: the note's first bold phrase, else its first sentence.
function stepTitle(body) {
  const bold = body.match(/\*\*(.+?)\*\*/);
  const text = (bold ? bold[1] : body.split(/(?<=[.!?])\s/)[0]).replace(/[`*_]/g, "").trim();
  return text.length > 70 ? text.slice(0, 69) + "…" : text;
}

// A heading reads bigger than the body text; VS Code has no font size setting for comments.
// The number also keeps the order readable in the Comments panel, which groups notes by file.
function noteMarkdown(step, i, n) {
  const rest = step.body.replace(/^\s*\*\*(.+?)\*\*\s*/, "");
  return `### ${i + 1}/${n} · ${stepTitle(step.body)}\n\n${rest}`;
}

// Put the notes of one trace on the code, and take the others off.
function render(traceId = store.shown) {
  for (const t of shownThreads.values()) t.dispose();
  shownThreads.clear();
  store.shown = traceId ?? null;
  const trace = store.traces[store.shown];
  if (trace && !trace.deletedAt) {
    const n = trace.steps.length;
    trace.steps.forEach((stepId, i) => {
      const step = store.steps[stepId];
      const range = new vscode.Range(step.startLine, 0, step.endLine, Number.MAX_SAFE_INTEGER);
      const thread = comments.createCommentThread(vscode.Uri.parse(step.uri), range, [
        comment(noteMarkdown(step, i, n), "Agent"),
        ...step.replies.map((r) => comment(r.text, r.author)),
      ]);
      thread.canReply = true;
      thread.label = `Step ${i + 1}/${n} · ${trace.title}`;
      thread.contextValue = `drawout ${stepId}${i === 0 ? " first" : ""}${i === n - 1 ? " last" : ""}`;
      thread.collapsibleState =
        stepId === currentStep ? vscode.CommentThreadCollapsibleState.Expanded : vscode.CommentThreadCollapsibleState.Collapsed;
      shownThreads.set(stepId, thread);
    });
  }
  save();
  traceChanged.fire();
  log(`render trace=${store.shown} notes=${shownThreads.size} traces=${Object.keys(store.traces).length}`);
}

const stepIdOf = (commentThread) => commentThread.contextValue.split(" ")[1];

// Open a step's file in the main editor group, select its lines and expand its note; fold the others.
async function focusStep(stepId, preserveFocus = false) {
  const step = store.steps[stepId];
  if (!step) return;
  currentStep = stepId;
  if (store.shown !== step.traceId) render(step.traceId);
  for (const [id, t] of shownThreads) {
    t.collapsibleState =
      id === stepId ? vscode.CommentThreadCollapsibleState.Expanded : vscode.CommentThreadCollapsibleState.Collapsed;
  }
  const range = shownThreads.get(stepId).range;
  const editor = await vscode.window.showTextDocument(vscode.Uri.parse(step.uri), {
    preview: false,
    preserveFocus,
    selection: range,
    viewColumn: vscode.ViewColumn.One,
  });
  editor.revealRange(range, vscode.TextEditorRevealType.InCenter);
}

async function showTrace(traceId) {
  const trace = store.traces[traceId];
  if (!trace) return;
  render(traceId);
  if (trace.steps.length) await focusStep(trace.steps[0], true);
}

// The step the arrow keys move from: the last one shown if it is in the active editor,
// else the step under the cursor, else the last one shown.
function stepAtCursor() {
  const editor = vscode.window.activeTextEditor;
  if (!editor) return currentStep;
  const here = [...shownThreads].filter(([, t]) => t.uri.toString() === editor.document.uri.toString());
  if (currentStep && here.some(([id]) => id === currentStep)) return currentStep;
  const line = editor.selection.active.line;
  return here.find(([, t]) => t.range.start.line <= line && line <= t.range.end.line)?.[0] ?? currentStep;
}

async function moveStep(fromStepId, delta) {
  const step = store.steps[fromStepId];
  if (!step) return;
  const ids = store.traces[step.traceId].steps;
  const next = ids[ids.indexOf(fromStepId) + delta];
  if (next) await focusStep(next);
}

// The chat in the sidebar changed: show that chat's current trace.
function setActiveThread(threadId) {
  if (threadId === activeThreadId) return;
  activeThreadId = threadId;
  const traceId = store.current[threadId];
  if (traceId && store.traces[traceId] && !store.traces[traceId].deletedAt) render(traceId);
  else traceChanged.fire();
}

// ---------- Trace view ----------

const traceChanged = new vscode.EventEmitter();
let traceView;
const traceTree = {
  onDidChangeTreeData: traceChanged.event,
  getChildren(el) {
    if (el?.kind === "trace") return store.traces[el.id].steps.map((id, i) => ({ kind: "step", id, i }));
    if (el?.kind === "deleted") return el.ids.map((id) => ({ kind: "trace", id }));
    if (el?.kind === "chat") return liveTraces(el.id).reverse().map((t) => ({ kind: "trace", id: t.id }));
    if (el) return [];
    // Root: the open chat's traces, or every chat when the open chat has none.
    const all = Object.values(store.traces);
    const chatIds = activeThreadId && all.some((t) => t.thread.id === activeThreadId) ? [activeThreadId] : [...new Set(all.map((t) => t.thread.id))];
    const roots =
      chatIds.length === 1
        ? liveTraces(chatIds[0]).reverse().map((t) => ({ kind: "trace", id: t.id }))
        : chatIds.reverse().map((id) => ({ kind: "chat", id }));
    const deleted = all.filter((t) => t.deletedAt && chatIds.includes(t.thread.id)).map((t) => t.id);
    if (deleted.length) roots.push({ kind: "deleted", ids: deleted });
    if (traceView) {
      const chat = chatIds.length === 1 && all.find((t) => t.thread.id === chatIds[0])?.thread.title;
      traceView.description = chat || "";
    }
    return roots;
  },
  getTreeItem(el) {
    const Collapsed = vscode.TreeItemCollapsibleState;
    if (el.kind === "chat") {
      const any = Object.values(store.traces).find((t) => t.thread.id === el.id);
      const item = new vscode.TreeItem(any.thread.title, Collapsed.Expanded);
      item.iconPath = new vscode.ThemeIcon("comment-discussion");
      return item;
    }
    if (el.kind === "deleted") {
      const item = new vscode.TreeItem("Deleted", Collapsed.Collapsed);
      item.iconPath = new vscode.ThemeIcon("trash");
      item.contextValue = "drawoutDeleted";
      return item;
    }
    if (el.kind === "trace") {
      const t = store.traces[el.id];
      const shown = store.shown === t.id;
      const item = new vscode.TreeItem(t.title, shown ? Collapsed.Expanded : Collapsed.Collapsed);
      item.id = t.id;
      item.description = `${t.steps.length} steps${shown ? " · shown" : ""}${store.current[t.thread.id] === t.id ? " · current" : ""}`;
      item.iconPath = new vscode.ThemeIcon(t.deletedAt ? "circle-slash" : shown ? "eye" : "list-ordered");
      item.contextValue = t.deletedAt ? "drawoutTraceDeleted" : "drawoutTrace";
      if (!t.deletedAt) item.command = { command: "drawOut.showTrace", title: "Show trace", arguments: [t.id] };
      return item;
    }
    const step = store.steps[el.id];
    const item = new vscode.TreeItem(`${el.i + 1}. ${stepTitle(step.body)}`);
    item.id = el.id;
    item.description = `${path.basename(vscode.Uri.parse(step.uri).fsPath)}:${step.startLine + 1}`;
    item.tooltip = new vscode.MarkdownString(step.body);
    item.command = { command: "drawOut.showStep", title: "Show step", arguments: [el.id] };
    return item;
  },
};

// Gabriel's own actions on traces, from the Trace view's context menu.
async function renameTrace(el) {
  const t = store.traces[el?.id];
  const title = t && (await vscode.window.showInputBox({ prompt: "Trace title", value: t.title }));
  if (!title) return;
  t.title = title;
  render();
}
function deleteTrace(el) {
  const t = store.traces[el?.id];
  if (!t) return;
  t.deletedAt = Date.now();
  if (store.current[t.thread.id] === t.id) delete store.current[t.thread.id];
  render(store.shown === t.id ? null : store.shown);
}
function restoreTrace(el) {
  const t = store.traces[el?.id];
  if (!t) return;
  t.deletedAt = null;
  render();
}
async function emptyDeleted(el) {
  const ids = el?.ids ?? [];
  const pick = await vscode.window.showWarningMessage(`Delete ${ids.length} traces for good?`, { modal: true }, "Delete");
  if (pick !== "Delete") return;
  for (const id of ids) {
    for (const stepId of store.traces[id].steps) delete store.steps[stepId];
    delete store.traces[id];
  }
  render();
}

// ---------- Editor tools ----------

// A relative path is relative to the calling thread's folder (X-T3-Thread-Cwd), which can be a T3 worktree
// of the repo this window has open. Without that header, it is relative to the window's first folder.
function resolveUri(p, cwd) {
  if (path.isAbsolute(p)) return vscode.Uri.file(p);
  if (cwd) return vscode.Uri.file(path.join(cwd, p));
  const root = vscode.workspace.workspaceFolders?.[0];
  if (!root) throw new Error("no workspace folder for a relative path");
  return vscode.Uri.joinPath(root.uri, p);
}

// 1-based inclusive lines in, 0-based lines out, clamped to the file.
async function lineRange(uri, startLine, endLine) {
  const doc = await vscode.workspace.openTextDocument(uri);
  const start = Math.min(doc.lineCount, Math.max(1, startLine ?? 1)) - 1;
  const end = Math.min(doc.lineCount, Math.max(start + 1, endLine ?? startLine ?? 1)) - 1;
  return { doc, start, end };
}

// The calling chat. The patched T3 names it in X-T3-Thread-Id; stock T3 needs the guess.
async function callingThread({ t3ThreadId }, toolName, args) {
  let t = null;
  try {
    if (t3ThreadId) {
      const shell = await t3Shell();
      const found = shell.threads.find((x) => x.id === t3ThreadId);
      t = found ? { ...found, v2: isV2(shell) } : { id: t3ThreadId, title: "Chat", v2: true };
    } else {
      t = await findCallingThread(toolName, args);
    }
  } catch (e) {
    log(`callingThread failed: ${e.message}`);
  }
  const thread = t ?? { id: "none", title: "No chat" };
  return { id: thread.id, title: thread.title, v2: !!thread.v2, runtimeMode: thread.runtimeMode, interactionMode: thread.interactionMode };
}

function createTrace(thread, title) {
  const trace = { id: newId("t"), title, thread, createdAt: Date.now(), deletedAt: null, steps: [] };
  store.traces[trace.id] = trace;
  store.current[thread.id] = trace.id;
  return trace;
}

// A trace of the calling chat, by id. The agent cannot touch other chats' traces.
function ownTrace(thread, traceId) {
  const t = store.traces[traceId];
  if (!t || t.thread.id !== thread.id) throw new Error(`no trace ${traceId} in this chat; call trace_list`);
  return t;
}
function ownStep(thread, stepId) {
  const s = store.steps[stepId];
  if (!s) throw new Error(`no step ${stepId}; call trace_list`);
  ownTrace(thread, s.traceId);
  return s;
}

const describeTrace = (t) =>
  `${t.id} "${t.title}"${t.deletedAt ? " (deleted)" : ""}: ` +
  (t.steps.map((id, i) => `${i + 1}. ${id} ${stepTitle(store.steps[id].body)}`).join("; ") || "no steps");

const lineProps = {
  path: { type: "string", description: "Absolute, or relative to this chat's folder." },
  startLine: { type: "number", description: "1-based." },
  endLine: { type: "number", description: "1-based, inclusive." },
};

const tools = {
  editor_open: {
    description: "Open a file as an editor tab, select a line range and scroll it to the center. No note.",
    inputSchema: { type: "object", properties: lineProps, required: ["path"] },
    async run({ path: p, startLine, endLine }, ctx) {
      const { doc, start, end } = await lineRange(resolveUri(p, ctx.cwd), startLine, endLine);
      const range = new vscode.Range(start, 0, end, doc.lineAt(end).text.length);
      const editor = await vscode.window.showTextDocument(doc, { preview: false, preserveFocus: true, selection: range, viewColumn: vscode.ViewColumn.One });
      editor.revealRange(range, vscode.TextEditorRevealType.InCenter);
      return `opened ${p}:${start + 1}-${end + 1}`;
    },
  },
  editor_focus: {
    description: "Bring one open file to the front of its editor group. Other tabs stay open behind it.",
    inputSchema: { type: "object", properties: { path: lineProps.path }, required: ["path"] },
    async run({ path: p }, ctx) {
      const uri = resolveUri(p, ctx.cwd);
      const tab = vscode.window.tabGroups.all.flatMap((g) => g.tabs).find((t) => t.input?.uri?.fsPath === uri.fsPath);
      await vscode.window.showTextDocument(uri, { preview: false, preserveFocus: true, viewColumn: tab?.group.viewColumn });
      return `focused ${p}${tab ? "" : " (was not open)"}`;
    },
  },
  editor_annotate: {
    description:
      "Add a step to a trace: a note on a line range. It goes to this chat's current trace, or to traceId. " +
      "Gabriel can reply on the note; the reply arrives in this chat as a message. Returns the step id.",
    inputSchema: {
      type: "object",
      properties: { ...lineProps, body: { type: "string", description: "Markdown. Start with a bold title." }, traceId: { type: "string" } },
      required: ["path", "startLine", "body"],
    },
    async run({ path: p, startLine, endLine, body, traceId }, ctx) {
      const uri = resolveUri(p, ctx.cwd);
      const { start, end } = await lineRange(uri, startLine, endLine);
      const thread = await callingThread(ctx, "editor_annotate", { body });
      let trace = traceId ? ownTrace(thread, traceId) : store.traces[store.current[thread.id]];
      if (!trace || trace.deletedAt) trace = createTrace(thread, `${thread.title} #${liveTraces(thread.id).length + 1}`);
      const step = { id: newId("s"), traceId: trace.id, uri: uri.toString(), startLine: start, endLine: end, body, replies: [] };
      store.steps[step.id] = step;
      trace.steps.push(step.id);
      // Show the first step of each burst of calls; the arrows reach the rest. A burst ends after 20 s.
      const now = Date.now();
      const burstStart = now - (lastStepAt.get(trace.id) ?? 0) > 20_000;
      lastStepAt.set(trace.id, now);
      if (burstStart) currentStep = step.id;
      render(trace.id);
      if (burstStart) await focusStep(step.id, true);
      log(`annotate ${step.id} ${p}:${start + 1} trace=${trace.id} chat=${thread.id} step=${trace.steps.length}`);
      return `step ${trace.steps.length} of trace ${trace.id} "${trace.title}", step id ${step.id}, on ${p}:${start + 1}-${end + 1}`;
    },
  },
  trace_create: {
    description:
      "Start a new trace in this chat and make it current: later editor_annotate calls add to it. " +
      "Start one for each new question; keep adding to the current one while the question stays the same.",
    inputSchema: { type: "object", properties: { title: { type: "string", description: "2-6 words." } }, required: ["title"] },
    async run({ title }, ctx) {
      const trace = createTrace(await callingThread(ctx, "trace_create", {}), title);
      render(trace.id);
      return `trace ${trace.id} "${title}" is current`;
    },
  },
  trace_list: {
    description: "List this chat's traces with their step ids, the current one first. Deleted traces are marked.",
    inputSchema: { type: "object", properties: {} },
    async run(_, ctx) {
      const thread = await callingThread(ctx, "trace_list", {});
      const mine = Object.values(store.traces).filter((t) => t.thread.id === thread.id);
      if (!mine.length) return "no traces in this chat";
      const cur = store.current[thread.id];
      return mine.sort((a, b) => (a.id === cur ? -1 : b.id === cur ? 1 : 0)).map((t) => (t.id === cur ? "current: " : "") + describeTrace(t)).join("\n");
    },
  },
  trace_select: {
    description: "Make one of this chat's traces current, and show it in the editor.",
    inputSchema: { type: "object", properties: { traceId: { type: "string" } }, required: ["traceId"] },
    async run({ traceId }, ctx) {
      const trace = ownTrace(await callingThread(ctx, "trace_select", {}), traceId);
      trace.deletedAt = null;
      store.current[trace.thread.id] = trace.id;
      await showTrace(trace.id);
      return `trace ${trace.id} "${trace.title}" is current`;
    },
  },
  trace_rename: {
    description: "Rename one of this chat's traces.",
    inputSchema: { type: "object", properties: { traceId: { type: "string" }, title: { type: "string" } }, required: ["traceId", "title"] },
    async run({ traceId, title }, ctx) {
      ownTrace(await callingThread(ctx, "trace_rename", {}), traceId).title = title;
      render();
      return `renamed ${traceId}`;
    },
  },
  trace_delete: {
    description: "Delete one of this chat's traces. It moves to Deleted in Gabriel's Trace view, where he can restore it.",
    inputSchema: { type: "object", properties: { traceId: { type: "string" } }, required: ["traceId"] },
    async run({ traceId }, ctx) {
      deleteTrace({ id: ownTrace(await callingThread(ctx, "trace_delete", {}), traceId).id });
      return `deleted ${traceId}; Gabriel can restore it`;
    },
  },
  step_edit: {
    description: "Change a step's note or its lines. Give only what changes.",
    inputSchema: { type: "object", properties: { stepId: { type: "string" }, body: { type: "string" }, ...lineProps }, required: ["stepId"] },
    async run({ stepId, body, path: p, startLine, endLine }, ctx) {
      const step = ownStep(await callingThread(ctx, "step_edit", {}), stepId);
      if (body) step.body = body;
      if (p || startLine) {
        const uri = p ? resolveUri(p, ctx.cwd) : vscode.Uri.parse(step.uri);
        const { start, end } = await lineRange(uri, startLine ?? step.startLine + 1, endLine ?? (startLine ? undefined : step.endLine + 1));
        Object.assign(step, { uri: uri.toString(), startLine: start, endLine: end });
      }
      render(store.shown === step.traceId ? step.traceId : store.shown);
      return `edited ${stepId}`;
    },
  },
  step_move: {
    description: "Move a step to another position in its trace (1-based).",
    inputSchema: { type: "object", properties: { stepId: { type: "string" }, position: { type: "number" } }, required: ["stepId", "position"] },
    async run({ stepId, position }, ctx) {
      const step = ownStep(await callingThread(ctx, "step_move", {}), stepId);
      const ids = store.traces[step.traceId].steps;
      ids.splice(ids.indexOf(stepId), 1);
      ids.splice(Math.max(0, Math.min(ids.length, position - 1)), 0, stepId);
      render(store.shown === step.traceId ? step.traceId : store.shown);
      return `moved ${stepId} to ${ids.indexOf(stepId) + 1}`;
    },
  },
  step_delete: {
    description: "Remove a step from its trace.",
    inputSchema: { type: "object", properties: { stepId: { type: "string" } }, required: ["stepId"] },
    async run({ stepId }, ctx) {
      const step = ownStep(await callingThread(ctx, "step_delete", {}), stepId);
      const ids = store.traces[step.traceId].steps;
      ids.splice(ids.indexOf(stepId), 1);
      delete store.steps[stepId];
      render(store.shown === step.traceId ? step.traceId : store.shown);
      return `removed ${stepId}`;
    },
  },
};

async function onReply(reply) {
  const stepId = stepIdOf(reply.thread);
  const step = store.steps[stepId];
  if (!step) return;
  const trace = store.traces[step.traceId];
  step.replies.push({ author: "Gabriel", text: reply.text });
  reply.thread.comments = [...reply.thread.comments, comment(reply.text, "Gabriel")];
  save();
  if (trace.thread.id === "none") {
    vscode.window.showWarningMessage("Draw-out: this trace has no T3 chat.");
    return;
  }
  const i = trace.steps.indexOf(stepId);
  const file = vscode.workspace.asRelativePath(vscode.Uri.parse(step.uri));
  const text =
    `Reply on step ${i + 1} (${stepId}) of trace "${trace.title}" (${trace.id}), ${file}:${step.startLine + 1}-${step.endLine + 1}\n\n` +
    `> ${step.body.split("\n").join("\n> ")}\n\n${reply.text}`;
  try {
    await sendToThread(trace.thread, text);
    log(`reply ${stepId} sent to ${trace.thread.id}`);
  } catch (e) {
    log(`reply failed: ${e.message}`);
    vscode.window.showErrorMessage(`Draw-out: reply failed: ${e.message}`);
  }
}

// ---------- MCP over HTTP (JSON responses, no SSE) ----------

function startMcp() {
  const server = http.createServer(async (req, res) => {
    if (req.method !== "POST" || !req.url.startsWith("/mcp")) return res.writeHead(405).end();
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const msg = JSON.parse(raw);
    if (msg.id === undefined) return res.writeHead(202).end(); // notification
    const reply = (result) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ jsonrpc: "2.0", id: msg.id, ...result }));
    };
    switch (msg.method) {
      case "initialize":
        return reply({
          result: {
            protocolVersion: msg.params?.protocolVersion ?? "2025-06-18",
            capabilities: { tools: {} },
            serverInfo: { name: "draw-out", version: "0.0.1" },
            instructions: [
              "Draw-out shows code in Gabriel's VS Code window, next to this chat.",
              "Whenever your answer points at code (how something works, where a bug is, what a change does), show the code there:",
              "call editor_annotate once per step, in reading order, each on a short line range with a 1-3 sentence note.",
              "Do this before you answer, and keep the chat answer short: the steps carry the detail. Do not paste the code in the chat.",
              "Steps form traces: numbered steps with arrows between them. A chat can have several traces.",
              "Call trace_create with a short title when a new question starts; keep adding to the current trace while the question stays the same.",
              "Use trace_list to see this chat's traces and step ids, and step_edit, step_move, step_delete, trace_rename, trace_select and trace_delete to change them.",
              "Use editor_open to show a file without a note. A reply to a note arrives in this chat as a message that quotes the note.",
              "These editor steps come first, even when you also draw an overview with html_render: the page shows the shape, the steps show the real code.",
              "Never answer a question about this repo's code with html_render alone.",
              "Write each note like this: start with a bold title of 2-5 words, then 1-3 sentences.",
              "Give a little context first: what this step does in the flow, before the detail.",
              "Use ASD-STE100 Simplified Technical English: one idea per sentence, 20 words at most, active voice, present tense.",
              "Use the words of the repo's CONTEXT.md when it has one. Put names from the code in `backticks`, and bold the one key term of the note.",
            ].join(" "),
          },
        });
      case "ping":
        return reply({ result: {} });
      case "tools/list":
        return reply({
          result: {
            tools: Object.entries(tools).map(([name, t]) => ({ name, description: t.description, inputSchema: t.inputSchema })),
          },
        });
      case "tools/call": {
        const tool = tools[msg.params.name];
        log(`tool ${msg.params.name} thread=${req.headers["x-t3-thread-id"] ?? "?"} ${JSON.stringify(msg.params.arguments)}`);
        try {
          const text = await tool.run(msg.params.arguments ?? {}, {
            t3ThreadId: req.headers["x-t3-thread-id"],
            cwd: req.headers["x-t3-thread-cwd"],
          });
          return reply({ result: { content: [{ type: "text", text }] } });
        } catch (e) {
          log(`tool error ${e.message}`);
          return reply({ result: { content: [{ type: "text", text: e.message }], isError: true } });
        }
      }
      default:
        return reply({ error: { code: -32601, message: `unknown method ${msg.method}` } });
    }
  });
  server.listen(cfg().get("mcpPort"), "127.0.0.1", () => {
    log(`mcp on http://127.0.0.1:${server.address().port}/mcp`);
    writeBridge(server);
  });
  return server;
}

function writeBridge(server) {
  const bridge = {
    version: 1,
    name: "draw-out",
    url: `http://127.0.0.1:${server.address().port}/mcp`,
    pid: process.pid,
    workspaceFolders: (vscode.workspace.workspaceFolders || []).map((f) => f.uri.fsPath),
  };
  removeBridge();
  bridgeFile = path.join(t3BaseDir(), "editor-bridges", `draw-out-${process.pid}.json`);
  fs.mkdirSync(path.dirname(bridgeFile), { recursive: true });
  fs.writeFileSync(bridgeFile, JSON.stringify(bridge, null, 2));
  log(`bridge ${bridgeFile} folders=${bridge.workspaceFolders.join(",")}`);
}

function removeBridge() {
  try {
    if (bridgeFile) fs.unlinkSync(bridgeFile);
  } catch {}
}

function activate(context) {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  output = vscode.window.createOutputChannel("Draw-out");
  comments = vscode.comments.createCommentController("draw-out", "Draw-out");
  loadStore(context.workspaceState);
  traceView = vscode.window.createTreeView("drawOut.trace", { treeDataProvider: traceTree });
  render();
  const mcp = startMcp();
  context.subscriptions.push(
    output,
    comments,
    { dispose: () => (removeBridge(), mcp.close(), proxyServer?.close()) },
    vscode.workspace.onDidChangeWorkspaceFolders(() => mcp.listening && writeBridge(mcp)),
    vscode.window.registerWebviewViewProvider("drawOut.chat", chatViewProvider, {
      webviewOptions: { retainContextWhenHidden: true },
    }),
    traceView,
    vscode.commands.registerCommand("drawOut.showTrace", showTrace),
    vscode.commands.registerCommand("drawOut.renameTrace", renameTrace),
    vscode.commands.registerCommand("drawOut.deleteTrace", deleteTrace),
    vscode.commands.registerCommand("drawOut.restoreTrace", restoreTrace),
    vscode.commands.registerCommand("drawOut.emptyDeleted", emptyDeleted),
    vscode.commands.registerCommand("drawOut.openChat", openChat),
    vscode.commands.registerCommand("drawOut.reloadChat", () => chatView && loadChat(chatView.webview)),
    vscode.commands.registerCommand("drawOut.showStep", focusStep),
    vscode.commands.registerCommand("drawOut.stepLeft", () => moveStep(stepAtCursor(), -1)),
    vscode.commands.registerCommand("drawOut.stepRight", () => moveStep(stepAtCursor(), 1)),
    vscode.commands.registerCommand("drawOut.reply", onReply),
    vscode.commands.registerCommand("drawOut.prevStep", (thread) => moveStep(stepIdOf(thread), -1)),
    vscode.commands.registerCommand("drawOut.nextStep", (thread) => moveStep(stepIdOf(thread), 1)),
  );
  log("activated");
  // Spike only: lets an agent open the panel without a click. The file holds the chat mode.
  const autoOpen = path.join(STATE_DIR, "auto-open-chat");
  const readMode = () => fs.readFileSync(autoOpen, "utf8").trim() || undefined;
  if (fs.existsSync(autoOpen)) openChat(readMode());
  fs.watchFile(autoOpen, { interval: 500 }, (cur) => cur.size && openChat(readMode()));
  if (context.extensionMode === vscode.ExtensionMode.Development) {
    fs.watchFile(__filename, { interval: 500 }, () => vscode.commands.executeCommand("workbench.action.reloadWindow"));
  }
  context.subscriptions.push({ dispose: () => (fs.unwatchFile(autoOpen), fs.unwatchFile(__filename)) });
}

process.on("exit", removeBridge);

module.exports = { activate, deactivate: removeBridge };
