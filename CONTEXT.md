# Draw-out

A VS Code extension where agents show Gabriel code while he talks to them in T3, and where Draw-out checks that he understands it.

## Language

### Traces

**Trace**:
An ordered list of steps that answers one question. It can grow over many turns. A thread can have several traces; the agent adds to the current one unless it starts a new one.
_Avoid_: tour, flow, walkthrough

**Step**:
One stop of a trace: a line range in a file, with the agent's note and Gabriel's replies. A step has one note, with a tone (finding, claim, info).
_Avoid_: card, mark, annotation, comment

**Saved trace**:
A trace written to `.draw-out/traces/` in the repo, so it outlasts the editor window and the thread.
_Avoid_: tour file, export

### Code structure

**Block**:
A named code thing a step can stop at: a handler, route, event, table, or an external service like Zoom. Plain helper functions are not blocks.
_Avoid_: landmark, node, component, resource

**Legend**:
A repo's own list of block kinds, and how to find each in its code. AI drafts it; it lives in the repo.
_Avoid_: schema, ontology

**Extractor**:
A script that reads a repo's code and finds the blocks of some kinds in its legend.
_Avoid_: parser, scanner

### Evidence

**Evidence**:
A pointer to a real source (code lines at a commit, ticket, PR, commit, message, log line, test run) that the UI fetches and renders. The agent never types the quoted text.
_Avoid_: quote, citation, reference

**Snapshot**:
The saved text of one piece of evidence, with its source, fetch time and version. Nobody edits it after the fetch.
_Avoid_: screenshot, cache

### Servers and windows

**Draw-out server**:
The patched T3 server that the chat panel shows. It attaches the editor tools to its threads.
_Avoid_: dev server, T3 fork

**Stock T3 server**:
An unpatched T3 server: the one behind the daily T3 app, the phone and agentbox.
_Avoid_: production T3 server, main server

**Editor window**:
A VS Code window that runs the extension. It receives the editor tool calls of threads in its folders.
_Avoid_: canvas host, client

**Chat panel**:
T3's web app inside a VS Code panel, connected to the Draw-out server.
_Avoid_: chat view, chat sidebar

**Thread history**:
How a thread evolved over time: messages, tool calls, files touched.
_Avoid_: thread trace (a trace answers a question about code)
