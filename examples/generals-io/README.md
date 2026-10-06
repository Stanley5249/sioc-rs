# generals-io

A simple bot for [generals.io](https://generals.io) built with `sioc`.

Demonstrates real-world use of stateful clients. The bot manages a complete game lifecycle using a state machine: queuing, in-game, game over, and rematch. It joins a game, sends a greeting, and surrenders around turn 5.

## Setup

- `GENERALS_IO_USER_ID` - Your generals.io user ID. In browser developers tools, open the Network tab (Fetch/XHR or Socket section) and find Socket.IO packets. Look for packets like `42["stars_and_rank","your-user-id"]`.

- `GENERALS_IO_USERNAME` - The username to set on an account that has none yet. The bot keeps an existing username, and an account without one needs this variable.

- `GENERALS_IO_MODE` - Optional. `private` joins a custom game with a random room name, which is the default, and `1v1` joins the public 1v1 queue.

- `RUST_LOG` - Log verbosity. See [filter directives](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives).

From the workspace root, copy `examples/.env.example` to `examples/.env`, fill in your user ID, and run the bot. `examples/.env` stays out of Git.

```sh
just examples::generals-io
```

Without `just`, set the variables in your shell. On Bash:

```bash
GENERALS_IO_USER_ID=your-user-id RUST_LOG=info cargo run --example glhf
```

On PowerShell:

```powershell
$env:GENERALS_IO_USER_ID = "your-user-id"; $env:RUST_LOG = "info"; cargo run --example glhf
```
