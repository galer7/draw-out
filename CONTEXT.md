# Draw-out

A VS Code extension where agents show Gabriel code while he talks to them in T3, and where Draw-out checks that he understands it.

## Language

### Traces

**Step**:
One stop of a trace: a line range in a file, with the agent's note and Gabriel's replies. A step has one note, with a tone (finding, claim, info).
_Avoid_: card, mark, annotation, comment

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
