# Quick Start Example

A self-contained demo of a Python Socket.IO server and a Rust client that exchange events and acknowledgements.

## Prerequisites

- uv with Python >= 3.14

## Running

Start the server:

```bash
uv run examples/quick-start/server.py
```

Then in a second terminal, run the client:

On Bash:

```bash
RUST_LOG=quick_start=info,sioc=trace cargo run --example quick_start
```

On PowerShell:

```powershell
$env:RUST_LOG="quick_start=info,sioc=trace"; cargo run --example quick_start
```
