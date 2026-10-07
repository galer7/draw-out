# Draw-out

A VS Code extension where agents open, focus and annotate code while Gabriel talks to them. T3 is the chat. Draw-out checks that Gabriel understands the code.

- Plan and open questions: [Map: Draw-out as a VS Code extension](https://github.com/galer7/draw-out/issues/1). Read it before you pick a ticket.
- v0 runs only on Gabriel's Mac: the agent, a T3 server and VS Code with the extension.
- T3 stays T3. Patches go to [`galer7/t3code`](https://github.com/galer7/t3code), a plain fork (local clone: `~/p/_forks/t3code`).

## Tickets

- The map asks for the `grilling` and `domain-modeling` skills on every ticket. They are not installed on the Mac: read them from `~/p/_ref/mattpocock-skills/skills/` (`productivity/grilling`, `engineering/domain-modeling`).
- Ask Gabriel few questions: one at a time, with a recommended answer.
- A spike or prototype goes on its own branch (`spike/<name>`), not on `main`. Its verdict goes on the ticket.

## Glossary

`CONTEXT.md` holds the domain words, when it exists. Use its words in code, tickets and replies.
