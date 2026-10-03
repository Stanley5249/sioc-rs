# Quick Start

A demo of a Rust client exchanging typed events, acknowledgements, and binary attachments with a Socket.IO server. The same client runs against two servers: `server.py` on python-socketio and `server.ts` on the reference JavaScript implementation.

The example covers all five concepts from the main README:

- **Open and Connect** — build a client and connect to the default namespace
- **Emit** — send a `Reply` event to the server
- **Listen** — receive `Greeting`, `Survey`, and `Chunk` events in a typed loop
- **Acks** — request an ack from the server (`Join`) and reply to one from the server (`Survey`)
- **Binary** — upload attachments (`Upload`) and receive one back (`Chunk`)

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) for `server.py`
- [Bun](https://bun.sh/docs/installation) for `server.ts`

Both servers take their dependencies from the workspace root `pyproject.toml` and `package.json`.

## Running

Run the client against each server in one command from the workspace root:

```bash
just smoke-py
just smoke-js
```

To run them by hand, start a server from the workspace root:

```bash
uv run examples/quick-start/server.py
# or
bun install && bun examples/quick-start/server.ts
```

Then in a second terminal, run the client.

On Bash:

```bash
RUST_LOG=quick_start=info,sioc=trace cargo run --example quick_start
```

On PowerShell:

```powershell
$env:RUST_LOG="quick_start=info,sioc=trace"; cargo run --example quick_start
```

## What Happens

1. The client connects and emits a `Join` event, waiting for a `RoomInfo` ack with the member count.
2. The client uploads `photo.png` as an `Upload` event with two binary attachments. The server answers with a `Chunk` event carrying one attachment.
3. The server sends a `Greeting` event. The client logs it and replies with a `Reply` event.
4. The server sends a `poll` event asking for a favorite language. The client finds `"Rust"` in the options and sends back a `Vote` ack.
5. The server disconnects the client and the loop exits cleanly. `server.py` disconnects the namespace, and `server.ts` closes the whole connection.
