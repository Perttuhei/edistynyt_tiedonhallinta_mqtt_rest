from fastapi import FastAPI
from endpoints import auth, device, sensors

app = FastAPI()

app.include_router(auth.router)
app.include_router(device.router)
app.include_router(sensors.router)
