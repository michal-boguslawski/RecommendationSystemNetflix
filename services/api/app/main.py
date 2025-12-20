# api/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.endpoints import info, recommend, health
from .api.deps import init_resources

def create_app() -> FastAPI:
    app = FastAPI(title="Recommendation API")

    app.add_event_handler("startup", init_resources)

    app.include_router(health.router, tags=["health"])
    app.include_router(recommend.router, tags=["recommend"])
    app.include_router(info.router, tags=["info"])

    # allow requests from UI
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app

app = create_app()
