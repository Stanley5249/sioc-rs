# Decisions

Mistakes this project already made and approaches the project rejected, each with the choice that replaced the mistake.

Write each entry in one or two sentences: what went wrong or was rejected, then what replaced it and why. Name behavior and symptoms, and leave private type and field names to the module docs. Keep the entries of one area in a tight list.

Each section covers one area or component. A rejected approach records why it failed there, not a ban elsewhere. Entries under Async and shutdown hold for every component.

## Async and shutdown

- A loop never awaits a send while it has other inputs to serve. One loop that served both directions stalled the heartbeat and deadlocked the engine and the manager once both inboxes filled, and the same loop deadlocked a caller that emits while handling events under a server flood.
- After a failure, the failing side keeps accepting input until the other side hangs up. Stopping at once made the other side fail with a closed-channel error, which replaced the root error.
- The end of a channel is the only close signal between tasks. Forwarding the Engine.IO `Close` packet as well gave two signals for one state, so every side had to handle both.
- Channels the server can fill are bounded, because an unbounded control channel queued a message for every repeated CONNECT or unknown DISCONNECT, so a server could grow it without limit.

## Manager actor

- Actor select handlers apply I/O results synchronously and yield only protocol inputs. A separate step enum and interpreter repeated outcomes the handlers had already decided.
- After a packet input, the actor hands off the message the core produced. Recomputing the read policy in the actor repeated a decision the core had already made.
- A cancelled output reservation is restarted, because the engine is the channel's only writer. Storing the pending future separately added state without another responsibility.
- One owner closes admission, drains the incoming direction, and collects the result, for normal closure, failure, and shutdown alike. Cancelling an opening drops its future, because a handshake may have no deadline.
- Packet selection keeps its output permit until the event is handed off. A readiness snapshot let engine polling revoke the permit after the event left its inbox, so cleanup discarded an unsent event.
- A local cancel of an opening aborts the engine task, but a peer-side failure keeps polling it after closing admission, so the original error survives. The engine cannot see a hang-up during opening once its transport channel is full, because the transport reads it only after the handshake.
- Peer closure processes the final control packets and discards later overflow, and local closure discards unread messages. Cleanup never waits for application capacity.
- One task owns the manager's state, and a synchronous core decides every transition. Two loops that shared namespace state behind a `Mutex` each saw half of every transition, which needed a three-step confirmation state and an unbounded event channel between them.
- A namespace's channel is its only send buffer, and the actor reads it only when the packet can go to the engine at once. Reading every packet into a second buffer in the core needed a semaphore to bound it, a disconnecting state to flush it, and per-event ack bookkeeping, and hiding the channel until the buffer had room hid its end, so an unconfirmed namespace never closed.
- A server packet held for a full receiver never hides the engine's hang-up. The actor kept sending events into a session that only drained, which lost them, and an empty closed server direction blocked the engine's cleanup.
- Every sender of a namespace's channel is a `SocketSender`, whose `DropGuard` cancels the token when the last one drops. The actor leaves an unconfirmed channel unread, so a raw sender dropped without the token left the namespace open forever.
- Rejected: one input channel shared by all sources. The shared channel loses per-namespace EOF and backpressure, starves control behind data, and needs a forwarding task for engine messages.
- Rejected: generation counters or incarnation ids to tell old work from new. Old work is isolated by ownership and finished cleanup instead.

## Namespaces

- Wire framing and direct `SocketSender` construction stay internal. Exposing them made implementation details part of the public compatibility contract.
- Confirmed participation flushes accepted packets when the engine has room, even after admission closes, and unconfirmed closed participation discards them. The two cases stay separate because their close policies differ on purpose.
- Event and ACK channels live in one group that keeps their separate lifetimes. Calling the group a send buffer suggested that ACK replies persisted like buffered events.
- CONNECT is queued when a namespace activates on a running engine or a new engine opens. A separate due flag duplicated those transitions and made engine startup clear an intent it did not use.
- Cleanup finishes the old inbox and any held delivery before it opens a waiting request, so it never opens replacement handles. A held delivery keeps its original receiver and token instead of resolving the name again.
- Ack waiters live with the active participation, and only the per-name counter outlives it. Keeping waiters beside the persistent name let cleanup and new handles share mutable state.
- A namespace ends through the EOF of its inbox when the last `SocketSender` drops. A disconnect message sent from `Drop` was lost when the inbox was full.
- A namespace that ends before the actor took its inbox frees its name at once. Waiting for old handles to drain an inbox nobody polled left the name taken forever.
- A send that waits for inbox capacity fails once the namespace closes. Before the shared `CancellationToken`, such a send kept waiting after the namespace closed.
- The terminal DISCONNECT or CONNECT_ERROR travels on its own oneshot. Through the data queue, the terminal packet never arrived while the receiver was full, so shutdown hung.
- CONNECT_ERROR closes the namespace like DISCONNECT. Before, the namespace stayed open after a refusal and could not open again.
- Server packets for a closed namespace are discarded. Treating them as errors failed the whole client, because the server may still send them after the client left.
- DISCONNECT and ACK go out only for a namespace the server confirmed to the current engine, and each engine gets new channels. The socket.io server closes the whole connection on any other packet for a namespace it has not joined, so a stale packet from an earlier engine killed the new one.
- Events emitted before confirmation wait in a bounded buffer. The unbounded buffer let an unconfirmed namespace grow without limit.

## Acks

- Unsent events keep the namespace inbox, and ACK replies use a separate bounded input that ends with the engine. Buffering both in the persistent inbox replayed old replies on the next engine.
- Event and reply inputs take turns being polled first, so a reply flood cannot starve events. When both end in one poll, that poll reports completion, because returning pending left no wake and stalled cleanup.
- Replies submitted while unconfirmed are discarded. `AckId` stays a wire id without a session tag, and session isolation comes from the owned input source.
- Ack ids count per namespace name and never restart. Restarting at 0 on reopen let a late ack for the closed namespace answer an event of the new one.
- Rejected: a session tag on server ack ids. The tag was added and reverted, because the server draws ids from one counter per namespace that never resets, so the server already ignores a stale id, and the JS client sends acks without a check.
- Registering an ack reclaims waiters whose handles were dropped or timed out. Without that, unanswered requests accumulated waiters.

## Reconnection

- Exponential delay and jitter use finite floats that saturate at the type's limit. A fixed exponent ceiling stopped growth before large configured caps.
- At the delay ceiling, the code keeps the configured `Duration`. Converting `Duration::MAX` through floating-point seconds rounded past its range and panicked on a retry.
- Engine loss ends participation whose handles already closed before it decides to retry. Scheduling first let an immediate retry open an engine no live namespace needed.
- A jitter outside 0 to 1 counts as none, like JS. A factor above 1 produced negative delays.
- The attempt count restarts on a successful Engine.IO handshake. Restarting on namespace CONNECT differed from JS `Manager.onreconnect`, which counts only failed connection attempts in a row.
- Each handshake has a deadline. Without one, a stalled attempt never failed, so the client never reconnected.

## Binary

- Reconstruction stores the decoded event or ACK with one shared attachment counter and buffer. A second binary-kind enum repeated distinctions the decoded packet already carries.
- Ending a namespace consumes its remaining attachments without delivery. Resetting the decoder lost frame alignment, and keeping the name as a live destination let an old packet reach replacement handles.
- Placeholders are validated before attachments are retained, and a binary packet carries 1 to `MAX_ATTACHMENTS` attachments. Unchecked references reached application indexing code and could hold reconstruction open.

## Engine.IO

- A failed WebSocket upgrade falls back to long polling. Failing the session on upgrade errors dropped connections that polling could serve.
- Out-of-place Open, Pong, and Upgrade packets are logged and ignored, like the JS client. Failing the session on them was stricter than the JS client and ended working sessions.
- Packets batched after the polling handshake reach the engine. They were dropped, but engine.io-client decodes the handshake response as a payload, so a server may batch packets after Open.
- A synchronous core decides the session rules, and the engine task only moves items between bounded channels. The two protocol loops that awaited each send could not answer the heartbeat deadline while blocked on a full manager channel, so a stalled manager kept the session open forever.
- The engine reads a server frame before it holds a manager permit and keeps only that frame's message, so pings ahead of the next message are answered while the manager is full. A ping behind an undelivered message still waits, so a manager that stalls for a whole ping window times the session out.
- The transport stays its own future behind bounded channels, and the engine polls it by reference. Folding the polling and WebSocket loops into the engine task was rejected, because it moved transport concurrency and the upgrade into the engine and let unrelated branches cancel a request in flight.
- The boundary channels carry `ServerFrame` and `ClientFrame`, which cannot hold Close, Upgrade, or the probe, so the transport owns those packets by type. A shared `Frame` channel let the engine receive Close and needed a runtime check.
- The handshake travels as the first server frame. A separate oneshot gave an opening a second end signal beside the channel's end.
- A local close discards the message waiting for the manager, and only a peer end delivers it, because the manager that closed no longer reads for it.
- A WebSocket local close sends the Close packet and a close frame without waiting for the server's reply, like the JS client. Waiting let a silent server hold the close open.

## Configuration

- A zero channel capacity is a builder error. It used to panic inside Tokio.

## Tests

- Core tests drive the core by inputs and outputs. Tests that ran through a rig modeling the actor's channel reads repeated the actor's logic and broke whenever the core's boundary moved.
