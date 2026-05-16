import asyncio

import socketio
import uvicorn

sio = socketio.AsyncServer(async_mode="asgi")
app = socketio.ASGIApp(sio)


@sio.event
async def connect(sid, _environ):
    asyncio.create_task(session(sid))


@sio.event
async def join(sid, data):
    print(f"[server] {sid} joined room '{data}'")
    return (1,)


@sio.event
async def reply(sid, data):
    print(f"[server] reply from {sid}: {data}")


async def session(sid):
    await sio.emit("greeting", "Welcome to the lobby!", to=sid)

    options = ["Rust", "Python", "JavaScripts"]

    ack = await sio.call(
        "poll",
        ("Favorite language?", options),
        to=sid,
        timeout=5,
    )
    print(f"[server] poll vote from {sid}: option {options[ack]}")

    await sio.disconnect(sid)


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=3000)
