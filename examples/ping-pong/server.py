import asyncio

import socketio
import uvicorn

sio = socketio.AsyncServer(
    async_mode="asgi",
    logger=True,
    engineio_logger=True,
)
app = socketio.ASGIApp(sio)


@sio.event
async def connect(sid, _):
    asyncio.create_task(ping_client(sid))


async def ping_client(sid):
    count = 0
    while True:
        count += 1
        await sio.emit("ping", count, to=sid)
        await asyncio.sleep(1)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="localhost",
        port=3000,
    )
