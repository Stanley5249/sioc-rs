"""Quick-start Socket.IO server demonstrating typed events with a Rust client."""

import argparse
import asyncio
import logging
import shlex
from typing import Any

import socketio
import uvicorn

logger = logging.getLogger("quick_start_server")

sio = socketio.AsyncServer(async_mode="asgi")
app = socketio.ASGIApp(sio)

_background_tasks: set[asyncio.Task[None]] = set()


@sio.event
async def connect(sid: str, _environ: dict[str, Any]) -> None:
    """Handle incoming client connection and start the interactive session.

    Args:
        sid: Socket ID assigned to the connected client.
        _environ: WSGI/ASGI connection environment metadata.
    """
    task = asyncio.create_task(session(sid))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@sio.event
async def join(sid: str, data: str) -> tuple[int]:
    """Handle room join request from a client.

    Args:
        sid: Socket ID of the client joining the room.
        data: Name of the room to join.

    Returns:
        A 1-tuple containing the initial member count acknowledgment.
    """
    logger.info("%s joined room %r", sid, data)
    return (1,)


@sio.event
async def reply(sid: str, data: str) -> None:
    """Handle text reply sent by a client.

    Args:
        sid: Socket ID of the sender.
        data: Reply message payload.
    """
    logger.info("reply from %s: %s", sid, data)


@sio.event
async def upload(sid: str, name: str, header: bytes, body: bytes) -> None:
    """Handle binary file upload and respond with a processed data chunk.

    Args:
        sid: Socket ID of the uploading client.
        name: Name of the uploaded file.
        header: Header binary payload.
        body: Body binary payload.
    """
    logger.info(
        "upload from %s: %r header=%dB body=%dB",
        sid,
        name,
        len(header),
        len(body),
    )
    chunk = b"received: " + header[:4]
    await sio.emit("chunk", (name, chunk), to=sid)


async def session(sid: str) -> None:
    """Run an interactive session sequence with the connected client.

    Args:
        sid: Socket ID of the target client.
    """
    await sio.emit("greeting", "Welcome to the lobby!", to=sid)

    options = ["Rust", "Python", "JavaScripts"]

    ack = await sio.call(
        "poll",
        ("Favorite language?", options),
        to=sid,
        timeout=5,
    )
    if not isinstance(ack, int):
        msg = f"expected an int vote, got {ack!r}"
        raise TypeError(msg)
    logger.info("poll vote from %s: option %s", sid, options[ack])

    await sio.disconnect(sid)


async def serve_client(command: str) -> int:
    """Serve until a client command finishes, for smoke tests.

    The client starts only after uvicorn binds the port, so no sleep or retry
    is needed, and the server exits even when the client fails.

    Args:
        command: Shell-style client command line.

    Returns:
        The client exit code, or 1 when the server fails to start.
    """
    server = uvicorn.Server(uvicorn.Config(app, host="localhost", port=3000))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            return 1
        await asyncio.sleep(0.1)
    client = await asyncio.create_subprocess_exec(*shlex.split(command))
    code = await client.wait()
    server.should_exit = True
    await serving
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", help="run this command, then exit with its code")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.client:
        raise SystemExit(asyncio.run(serve_client(args.client)))
    uvicorn.run(app, host="localhost", port=3000)
