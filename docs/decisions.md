# Decisions

Mistakes this project already made and approaches the project rejected, each with the choice that replaced the mistake.

## Async and shutdown

- Actor select handlers apply I/O results synchronously and yield only protocol inputs. A separate `Step` enum and interpreter repeated those same outcomes without crossing another wait or defining a separate ownership boundary.
- After a packet input, the actor hands off the message the core produces on its owned slot. Recomputing the read policy in the actor repeated a protocol decision already made before the packet was dequeued.

- A pending engine output reservation belongs to one actor wait. The engine is the channel's only writer, so restarting a cancelled reservation loses no position to another waiter. Acquired permits stay owned until handoff; storing the pending future separately added a boxed object and cleanup state without another responsibility.

- One engine owner closes admission, drains the incoming direction and collects the connection result. Normal closure, internal failure and shutdown use this same path. Opening cancellation drops its pending future instead of waiting for a handshake that may have no deadline.
- Packet selection owns its output permit until the synchronous header handoff. Keeping only a readiness snapshot let concurrent engine polling revoke the permit after the event left its inbox, so engine cleanup discarded a truly unsent event. A close transition reports progress before waiting again, allowing the caller to return or release its owned slot.
- A local opening cancellation drops the connection future; a peer-side opening failure keeps polling it after closing admission so its original error survives input reclamation.
- Peer closure processes final control packets while reclaiming the session. An already-held packet keeps its original receiver, while later incoming overflow is discarded during cleanup. Local closure discards unread engine messages. This explicit drain policy prevents application capacity from becoming a cleanup prerequisite.

- A loop never awaits a send while it has other inputs to serve. One loop that served both directions stalled the heartbeat and deadlocked the engine and the manager once both inboxes filled, and the same loop deadlocked a caller that emits while handling events under a server flood.
- After a failure, the failing side keeps accepting input until the other side hangs up. Stopping at once made the other side fail with a closed-channel error, which replaced the root error.
- The end of a channel is the only close signal between tasks. Forwarding the Engine.IO `Close` packet as well gave two signals for one state, so every side had to handle both.
- Channels the server can fill are bounded, because an unbounded control channel queued a message for every repeated CONNECT or unknown DISCONNECT, so a server could grow it without limit.
- One task owns the manager's state, and a synchronous core decides every transition. Two loops that shared namespace state behind a `Mutex` each saw half of every transition, which needed a three-step confirmation state and an unbounded event channel between them.
- A namespace's channel is its only send buffer, and the actor reads it only when the packet can go to the engine at once. Reading every packet into a second buffer in the core needed a semaphore to bound it, a disconnecting state to flush it, and per-event ack bookkeeping, and hiding the channel until the buffer had room hid its end, so an unconfirmed namespace never closed.
- A server packet held for a full receiver never hides the engine's hang-up. The actor kept sending events into a session that only drained, which lost them, and an empty closed server direction blocked the engine's cleanup.
- Every sender of a namespace's channel is a `SocketSender`, whose `DropGuard` cancels the token when the last one drops. The actor leaves an unconfirmed channel unread, so a raw sender dropped without the token left the namespace open forever.
- Rejected: one input channel shared by all sources. The shared channel loses per-namespace EOF and backpressure, starves control behind data, and needs a forwarding task for engine messages.
- Rejected: generation counters or incarnation ids to tell old work from new. Old work is isolated by ownership and finished cleanup instead.

## Namespaces

- Read policy handles confirmed participation first: it flushes accepted packets when engine capacity is available, even after admission closes. Unconfirmed closed participation discards them. Keeping those cases separate makes the intentional close-policy asymmetry explicit.

- The actor owns `NamespaceInputs`, which groups the event and ACK channels while preserving their different lifetimes. Naming the whole bundle `SendBuffer` suggested that ACK replies persisted with buffered events.

- CONNECT is queued when participation activates on a running engine, or when a new engine opens for all active namespaces. A separate `connect_due` flag duplicated those transitions and made engine startup clear an enqueue intention it did not use.

- Namespace cleanup never opens replacement handles. The core finishes old inbox ownership and held delivery before opening the waiting request. Repeated terminal packets keep the old participation draining. Held deliveries own the original receiver and token rather than resolving the namespace name again. That token gives post-transition cleanup one path to discard a delivery before reopening.
- Ack waiters live with active participation; only the per-name ack counter outlives it. Keeping waiters beside the persistent name allowed cleanup and new handles to share mutable state.

- A namespace ends through the EOF of its inbox when the last `SocketSender` drops. A disconnect message sent from `Drop` was lost when the inbox was full.
- A namespace that ends before the actor took its inbox frees its name at once. Waiting for old handles to drain an inbox nobody polled left the name taken forever.
- A send that waits for inbox capacity fails once the namespace closes. Before the shared `CancellationToken`, such a send kept waiting after the namespace closed.
- The terminal DISCONNECT or CONNECT_ERROR travels on its own oneshot. Through the data queue, the terminal packet never arrived while the receiver was full, so shutdown hung.
- CONNECT_ERROR closes the namespace like DISCONNECT. Before, the namespace stayed open after a refusal and could not open again.
- Server packets for a closed namespace are discarded. Treating them as errors failed the whole client, because the server may still send them after the client left.
- DISCONNECT and ACK go out only for a namespace the server confirmed to the current engine, and each engine gets new channels. The socket.io server closes the whole connection on any other packet for a namespace it has not joined, so a stale packet from an earlier engine killed the new one.
- Events emitted before confirmation wait in a bounded buffer. The unbounded buffer let an unconfirmed namespace grow without limit.

## Acks

- Unsent events keep their namespace inbox, while ACK replies use a separate bounded input for the confirmed engine. Engine completion drops that input before reconnection, including blocked submissions that captured its sender. Buffering both kinds in the persistent event inbox silently replayed old replies on the next engine. Each input has the configured manager capacity, and local disconnect drains both before releasing the namespace name.
- Event and reply inputs alternate their first polling opportunity, so a reply flood cannot starve buffered events. When both inputs end in one poll, that poll reports completion; returning pending after observing both EOFs left no future wake and stalled namespace cleanup.
- Replies submitted while unconfirmed are discarded, preserving the existing rule. `AckId` stays a wire id without a session tag; session isolation applies to queued submissions through their owned input source rather than changing the public event API.

- Ack ids count per namespace name and never restart. Restarting at 0 on reopen let a late ack for the closed namespace answer an event of the new one.
- Rejected: a session tag on server ack ids. The tag was added and reverted, because the server draws ids from one counter per namespace that never resets, so the server already ignores a stale id, and the JS client sends acks without a check.
- Registering an ack reclaims waiters whose handles were dropped or timed out. Without that, unanswered requests accumulated waiters.

## Reconnection

- Exponential delay and jitter use finite floating-point operands. A fixed
  exponent ceiling stopped growth before large configured caps; saturating at
  the numeric type's limit preserves zero delays and lets the configured cap
  decide when positive delays stop growing.

- At the reconnection delay ceiling, keep the configured `Duration` itself.
  Floating-point conversion can round `Duration::MAX` beyond its valid range,
  so converting the ceiling back from seconds panicked on a retry.

- Engine loss ends participation whose handles already closed before deciding whether to retry. Scheduling first let an immediate retry win the race against inbox EOF and open an engine that no live namespace needed.

- A jitter outside 0 to 1 counts as none, like JS. A factor above 1 produced negative delays.
- The attempt count restarts on a successful Engine.IO handshake. Restarting on namespace CONNECT differed from JS `Manager.onreconnect`, which counts only failed connection attempts in a row.
- Each handshake has a deadline. Without one, a stalled attempt never failed, so the client never reconnected.

## Binary

- Reconstruction stores the already-decoded event or ACK and shares one attachment counter and buffer. Repeating payloads, ids and buffers in a second binary-kind enum duplicated distinctions the decoded packet already carries.

- Ending a namespace detaches its partial binary packet and consumes the remaining attachments without delivery. Resetting the decoder would lose frame alignment, while retaining the name as a live destination let an old packet reach replacement handles.

- Placeholders are validated before attachments are retained, and a binary packet carries 1 to `MAX_ATTACHMENTS` attachments. Unchecked references reached application indexing code and could hold reconstruction open.

## Engine.IO

- A failed WebSocket upgrade falls back to long polling. Failing the session on upgrade errors dropped connections that polling could serve.
- Out-of-place Open, Pong, and Upgrade packets are logged and ignored, like the JS client. Failing the session on them was stricter than the JS client and ended working sessions.
- Packets batched after the polling handshake reach the engine. They were dropped, but engine.io-client decodes the handshake response as a payload, so a server may batch packets after Open.

## Configuration

- A zero channel capacity is a builder error. It used to panic inside Tokio.

## Tests

- Core tests drive the core by inputs and outputs. Tests that ran through a rig modeling the actor's channel reads repeated the actor's logic and broke whenever the core's boundary moved.
