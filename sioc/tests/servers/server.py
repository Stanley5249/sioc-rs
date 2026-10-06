"""Reference server for protocol and backpressure integration tests."""

import socket
import sys
import threading

import socketio
import socketio.exceptions
import uvicorn

sio = socketio.AsyncServer(
    async_mode="asgi", max_http_buffer_size=256, allow_upgrades=False
)
app = socketio.ASGIApp(sio)
counts: dict[str, int] = {}


@sio.event
async def connect(sid: str, _environ: dict[str, object]) -> None:
    """Initialize the connection's receive count."""
    counts[sid] = 0


async def observe(_sid: str, _environ: dict[str, object]) -> None:
    """Accept disconnect observers."""


async def denied(_sid: str, _environ: dict[str, object]) -> None:
    """Reject this namespace without ending the session."""
    message = "denied"
    raise socketio.exceptions.ConnectionRefusedError(message)


sio.on("connect", handler=observe, namespace="/observe")
sio.on("connect", handler=denied, namespace="/denied")


@sio.event
async def flood(sid: str, count: int) -> None:
    """Flood ordered events to the Rust client's bounded queues."""
    for seq in range(count):
        await sio.emit("item", seq, to=sid)


@sio.event
async def seen(sid: str, _seq: int) -> None:
    """Count echoes from a client under server pressure."""
    counts[sid] += 1


@sio.event
async def echo(sid: str, seq: int) -> None:
    """Echo events while the Rust client sends under receive pressure."""
    counts[sid] += 1
    await sio.emit("item", seq, to=sid)


@sio.event
async def count(sid: str) -> tuple[int]:
    """Return the exact number of events received from this client."""
    return (counts[sid],)


@sio.event
async def blob(sid: str, data: bytes) -> tuple[bytes]:
    """Echo binary attachments both as an event and as an acknowledgement."""
    await sio.emit("blob", data, to=sid)
    return (data,)


@sio.event
async def kick(sid: str) -> None:
    """Disconnect only this namespace."""
    await sio.disconnect(sid)


@sio.event
async def disconnect(sid: str, _reason: str) -> None:
    """Notify observers and release connection state."""
    counts.pop(sid, None)
    await sio.emit("gone", namespace="/observe")


def main() -> None:
    """Serve on an ephemeral port until the Rust test closes stdin."""
    # Binding here leaves no gap in which another process could take the port.
    listener = socket.create_server(("127.0.0.1", 0))
    # The Rust test reads the port from the first stdout line.
    sys.stdout.write(f"{listener.getsockname()[1]}\n")
    sys.stdout.flush()
    config = uvicorn.Config(app, log_level="error", timeout_graceful_shutdown=1)
    server = uvicorn.Server(config)

    # Closing stdin shuts down the server, including when a Rust test unwinds.
    def stop_on_eof() -> None:
        sys.stdin.read()
        server.should_exit = True

    threading.Thread(target=stop_on_eof, daemon=True).start()
    server.run(sockets=[listener])


if __name__ == "__main__":
    main()
