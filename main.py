from fleet_orchestrator.api import create_app

app = create_app()


@app.get("/")
def read_root():
    return {"service": "fleet-mission-orchestrator", "docs": "/docs"}
