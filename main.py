from fastapi import FastAPI

from app.api import router as api_router

app = FastAPI(title="Fleet Mission Orchestrator")
app.include_router(api_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
