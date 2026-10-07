# Quick Start

A chat client in Rust that joins a room, talks to a bot, and shares an image. Choose `server.ts` on the reference JavaScript implementation or `server.py` on `python-socketio`; both servers implement the same chat events. The bot stands in for other room members, so one client still sees broadcasts.

The example covers all five concepts from the main README:

- **Open and Connect** — build a client and connect to the default namespace
- **Emit** — send a `Say` message to the room
- **Listen** — receive `Message`, `Notice`, `Confirm`, and `Image` events in a typed loop
- **Acks** — request an ack from the server (`Join`) and answer one from the server (`Confirm`)
- **Binary** — send an `Image` attachment and receive it back

## Prerequisites

- For `server.ts`: [Bun](https://bun.sh/docs/installation), with dependencies from the workspace root `package.json`.
- For `server.py`: [uv](https://docs.astral.sh/uv/getting-started/installation/), which manages Python and dependencies from the workspace root `pyproject.toml` and `uv.lock`.

## Running

From the workspace root, choose one server and start the server in one terminal.

For TypeScript:

```bash
bun install --frozen-lockfile
just examples::quick-start-ts
```

For Python:

```sh
just examples::quick-start-py
```

Both recipes keep the server running on `127.0.0.1:3000`, so run one server at a time. In a second terminal, run the Rust client:

```sh
just examples::quick-start
```

Stop the server with Ctrl+C after the client finishes. `just test-e2e` runs protocol and pressure tests against the TypeScript reference fixture. CI includes those E2E tests in its combined Rust test run. Python formatting, linting, and type checking are included in `just ci-all`.

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
5. TypeScript closes the whole connection; Python disconnects the namespace. The Rust client exits the receive loop and closes the remaining session in both cases.
