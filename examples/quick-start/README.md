# Quick Start Example

A self-contained demo of the core sioc features: a Python Socket.IO server and a Rust client
that exchange typed events and acknowledgements.

## Prerequisites

- uv with Python >= 3.14

## Running

Start the server (from `examples/quick-start/`):

```bash
uv run server.py
```

Then in a second terminal, run the client (from the workspace root):

On Bash:

```bash
RUST_LOG=quick_start=info,sioc=trace cargo run --example quick_start
```

On PowerShell:

```powershell
$env:RUST_LOG="quick_start=info,sioc=trace"; cargo run --example quick_start
```
