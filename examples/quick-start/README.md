# Quick Start

A chat client in Rust that joins a room, talks to a bot, and shares an image with a Socket.IO server. The same client runs against two servers: `server.py` on python-socketio and `server.ts` on the reference JavaScript implementation. The bot stands in for other room members, so one client still sees broadcasts.

The example covers all five concepts from the main README:

- **Open and Connect** — build a client and connect to the default namespace
- **Emit** — send a `Say` message to the room
- **Listen** — receive `Message`, `Notice`, `Confirm`, and `Image` events in a typed loop
- **Acks** — request an ack from the server (`Join`) and answer one from the server (`Confirm`)
- **Binary** — send an `Image` attachment and receive it back

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

Each side logs only the events it receives, so every event appears once, with an arrow pointing at the receiver:

```text
server <- join      ferris wants room "rust"
client <- ack       join: 2 members
client <- notice    ferris joined rust
server <- image     crab.png (8 bytes)
server <- message   hello from Rust!
client <- image     crab.png (8 bytes)
client <- message   bot: hi ferris, you said "hello from Rust!"
client <- confirm   Leave the room?
server <- ack       confirm: yes
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

```text
 Rust client                                server (+ bot)
     |--- join {room, name} --------------------->|
     |<-- ack join {members} ---------------------|
     |<-- notice "ferris joined rust" ------------|
     |--- image {name, data} [binary] ----------->|
     |<-- image {name, data} [binary] ------------|
     |--- message {text} ------------------------>|
     |<-- message {from: "bot", text} ------------|
     |<-- confirm {question} ---------------------|
     |--- ack confirm true ---------------------->|
     |<-- disconnect -----------------------------|
```

1. The client emits `Join` and waits up to five seconds for a `RoomInfo` ack with the member count, which includes the bot. The server announces the newcomer with a `Notice` to the room.
2. The client shares `crab.png` as an `Image` event with one binary attachment. The server broadcasts it to the room, so the client receives it back.
3. The client sends a `Say` message. The server broadcasts it to other members, and the bot replies with a `Message`.
4. The server sends `Confirm` asking whether to leave the room. The client answers with an `Answer(true)` ack.
5. The server disconnects the client and the loop exits cleanly. `server.py` disconnects the namespace, and `server.ts` closes the whole connection.
