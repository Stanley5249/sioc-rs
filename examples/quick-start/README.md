# Quick Start

A self-contained demo of a Python Socket.IO server and a Rust client exchanging typed events and acknowledgements.

The example covers all four concepts from the main README:

- **Open and Connect** — build a client and connect to the default namespace
- **Emit** — send a `Reply` event to the server
- **Listen** — receive `Greeting` and `Survey` events in a typed loop
- **Acks** — request an ack from the server (`Join`) and reply to one from the server (`Survey`)

## Prerequisites

- uv with Python >= 3.14

## Running

Start the server:

```bash
cd examples/quick-start
uv run server.py
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

1. The client connects and immediately emits a `Join` event, waiting for a `RoomInfo` ack with the member count.
2. The server sends a `Greeting` event. The client logs it and replies with a `Reply` event.
3. The server sends a `Poll` event asking for a favorite language. The client finds `"Rust"` in the options and sends back a `Vote` ack.
4. The server disconnects the client and the loop exits cleanly.
