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
async function startProxy(rewriteCookies) {
  proxyServer?.close();
  const target = new URL(cfg().get("t3Url"));
  const fixCookie = (c) =>
    rewriteCookies ? c.replace(/;\s*SameSite=Lax/i, "") + "; SameSite=None; Secure" : c;

  proxyServer = http.createServer((req, res) => {
    const hasCookie = /t3_session_/.test(req.headers.cookie || "");
    const upstream = http.request(
      { host: target.hostname, port: target.port, path: req.url, method: req.method, headers: { ...req.headers, host: target.host } },
      (up) => {
        const headers = { ...up.headers };
        if (headers["set-cookie"]) headers["set-cookie"] = headers["set-cookie"].map(fixCookie);
        if (req.url.startsWith("/api/auth") || req.url === "/" || req.url.startsWith("/pair"))
          log(`proxy ${req.method} ${req.url} cookie=${hasCookie} -> ${up.statusCode}${headers["set-cookie"] ? " set-cookie" : ""}`);
        res.writeHead(up.statusCode, headers);
        up.pipe(res);
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

async function openChat(modeOverride) {
  const mode = typeof modeOverride === "string" ? modeOverride : cfg().get("chatMode");
  const base = mode === "direct" ? cfg().get("t3Url") : await startProxy(mode === "proxy-samesite-none");
  let src = base + "/";
  try {
    const { credential } = JSON.parse(await t3Cli(["auth", "pairing", "create", "--ttl", "5m", "--label", "draw-out-panel", "--json"]));
    src = `${base}/pair#token=${credential}`;
  } catch (e) {
    log(`pairing failed: ${e.message}`);
  }
  log(`openChat mode=${mode} base=${base}`);

  const panel = vscode.window.createWebviewPanel("drawOut.chat", "T3", vscode.ViewColumn.Beside, {
    enableScripts: true,
    retainContextWhenHidden: true,
  });
  panel.webview.html = `<!doctype html><html><head>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src http://127.0.0.1:* http://localhost:*; style-src 'unsafe-inline';">
<style>html,body,iframe{margin:0;padding:0;border:0;width:100%;height:100vh;overflow:hidden}</style>
</head><body><iframe src="${src}" allow="clipboard-read; clipboard-write"></iframe></body></html>`;
}

// ---------- Editor tools ----------

function resolveUri(p) {
  if (path.isAbsolute(p)) return vscode.Uri.file(p);
  const root = vscode.workspace.workspaceFolders?.[0];
  if (!root) throw new Error("no workspace folder for a relative path");
  return vscode.Uri.joinPath(root.uri, p);
}

// 1-based inclusive lines in, a vscode Range out.
function toRange(doc, startLine, endLine) {
  const s = Math.max(1, startLine ?? 1) - 1;
  const e = Math.min(doc.lineCount, endLine ?? startLine ?? 1) - 1;
  return new vscode.Range(s, 0, e, doc.lineAt(e).text.length);
}

let comments;
const threads = new Map(); // annotation id -> { thread, t3Thread, path, traceKey }
const traces = new Map(); // T3 thread id (or "none") -> annotation ids, in step order

// contextValue carries the annotation id, plus "first"/"last" so the arrow buttons can hide.
const annotationId = (thread) => thread.contextValue.split(" ")[1];

function relabel(traceKey) {
  const ids = traces.get(traceKey);
  ids.forEach((id, i) => {
    const { thread, t3Thread } = threads.get(id);
    thread.label = `Step ${i + 1}/${ids.length}` + (t3Thread ? ` · ${t3Thread.title}` : "");
    thread.contextValue = `drawout ${id}${i === 0 ? " first" : ""}${i === ids.length - 1 ? " last" : ""}`;
  });
}

async function goToStep(fromThread, delta) {
  const entry = threads.get(annotationId(fromThread));
  const ids = traces.get(entry.traceKey);
  const next = ids[ids.indexOf(annotationId(fromThread)) + delta];
  if (!next) return;
  for (const id of ids) {
    threads.get(id).thread.collapsibleState =
      id === next ? vscode.CommentThreadCollapsibleState.Expanded : vscode.CommentThreadCollapsibleState.Collapsed;
  }
  const { thread } = threads.get(next);
  const editor = await vscode.window.showTextDocument(thread.uri, { preview: false, selection: thread.range });
  editor.revealRange(thread.range, vscode.TextEditorRevealType.InCenter);
}

const tools = {
  editor_open: {
    description: "Open a file as an editor tab, select a line range and scroll it to the center.",
    inputSchema: {
      type: "object",
      properties: {
        path: { type: "string", description: "Absolute, or relative to the workspace root." },
        startLine: { type: "number", description: "1-based." },
        endLine: { type: "number", description: "1-based, inclusive." },
      },
      required: ["path"],
    },
    async run({ path: p, startLine, endLine }) {
      const doc = await vscode.workspace.openTextDocument(resolveUri(p));
      const range = toRange(doc, startLine, endLine);
      const editor = await vscode.window.showTextDocument(doc, { preview: false, preserveFocus: true, selection: range });
      editor.revealRange(range, vscode.TextEditorRevealType.InCenter);
      return `opened ${p}:${range.start.line + 1}-${range.end.line + 1}`;
    },
  },
  editor_focus: {
    description: "Bring one open file to the front of its editor group. Other tabs stay open behind it.",
    inputSchema: { type: "object", properties: { path: { type: "string" } }, required: ["path"] },
    async run({ path: p }) {
      const uri = resolveUri(p);
      const tab = vscode.window.tabGroups.all.flatMap((g) => g.tabs).find((t) => t.input?.uri?.fsPath === uri.fsPath);
      await vscode.window.showTextDocument(uri, { preview: false, preserveFocus: true, viewColumn: tab?.group.viewColumn });
      return `focused ${p}${tab ? "" : " (was not open)"}`;
    },
  },
  editor_annotate: {
    description:
      "Add a comment thread on a line range. Gabriel can reply in the thread; the reply arrives in this chat as a new message.",
    inputSchema: {
      type: "object",
      properties: {
        path: { type: "string" },
        startLine: { type: "number" },
        endLine: { type: "number" },
        body: { type: "string", description: "Markdown." },
      },
      required: ["path", "startLine", "body"],
    },
    async run({ path: p, startLine, endLine, body }, { t3ThreadId }) {
      const uri = resolveUri(p);
      const doc = await vscode.workspace.openTextDocument(uri);
      const range = toRange(doc, startLine, endLine);
      const thread = comments.createCommentThread(uri, range, [comment(body, "Agent")]);
      thread.canReply = true;
      thread.collapsibleState = vscode.CommentThreadCollapsibleState.Expanded;
      const id = crypto.randomUUID().slice(0, 8);
      let t3Thread = null;
      try {
        if (t3ThreadId) {
          const shell = await t3Shell();
          const t = shell.threads.find((t) => t.id === t3ThreadId);
          t3Thread = t ? { ...t, v2: isV2(shell) } : null;
        } else {
          t3Thread = await findCallingThread("editor_annotate", { body });
        }
      } catch (e) {
        log(`findCallingThread failed: ${e.message}`);
      }
      const traceKey = t3Thread?.id ?? "none";
      threads.set(id, { thread, t3Thread, path: p, traceKey });
      traces.set(traceKey, [...(traces.get(traceKey) ?? []), id]);
      relabel(traceKey);
      log(`annotate ${id} ${p}:${startLine} t3Thread=${traceKey} step=${traces.get(traceKey).length}`);
      return `step ${traces.get(traceKey).length}, annotation ${id} on ${p}:${range.start.line + 1}-${range.end.line + 1}; replies go to T3 thread ${t3Thread?.title ?? "(none found)"}`;
    },
  },
};

function comment(body, author) {
  return { body: new vscode.MarkdownString(body), mode: vscode.CommentMode.Preview, author: { name: author } };
}

async function onReply(reply) {
  const entry = threads.get(annotationId(reply.thread));
  reply.thread.comments = [...reply.thread.comments, comment(reply.text, "Gabriel")];
  if (!entry?.t3Thread) {
    vscode.window.showWarningMessage("Draw-out: this comment thread has no T3 thread.");
    return;
  }
  const r = entry.thread.range;
  const quote = reply.thread.comments.slice(-2, -1)[0]?.body.value ?? "";
  const text = `Reply on ${entry.path}:${r.start.line + 1}-${r.end.line + 1} (annotation ${annotationId(reply.thread)})\n\n> ${quote.split("\n").join("\n> ")}\n\n${reply.text}`;
  try {
    await sendToThread(entry.t3Thread, text);
    log(`reply ${annotationId(reply.thread)} sent to ${entry.t3Thread.id}`);
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
              "The notes of one chat form a trace with numbered steps and arrows between them.",
              "Use editor_open to show a file without a note. A reply to a note arrives in this chat as a message that quotes the note.",
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
          const text = await tool.run(msg.params.arguments ?? {}, { t3ThreadId: req.headers["x-t3-thread-id"] });
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
  const mcp = startMcp();
  context.subscriptions.push(
    output,
    comments,
    { dispose: () => (removeBridge(), mcp.close(), proxyServer?.close()) },
    vscode.workspace.onDidChangeWorkspaceFolders(() => mcp.listening && writeBridge(mcp)),
    vscode.commands.registerCommand("drawOut.openChat", openChat),
    vscode.commands.registerCommand("drawOut.reply", onReply),
    vscode.commands.registerCommand("drawOut.prevStep", (thread) => goToStep(thread, -1)),
    vscode.commands.registerCommand("drawOut.nextStep", (thread) => goToStep(thread, 1)),
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
