from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from common.logger import logger
from modules.assembly.api.routes import router as assembly_router
from modules.consumption.api.routes import router as consumption_router
from modules.forecast.api.routes import router as forecast_router
from modules.requests_builder.api.routes import router as requests_builder_router
from modules.requests_checker.api.routes import router as requests_checker_router
from modules.requests_closure.api.routes import router as requests_closure_router
from modules.sap_manager.api.routes import router as sap_manager_router
import uvicorn


log = logger("main")


def create_app() -> FastAPI:
    log.info("FastAPI app initialized")

    app = FastAPI(
        title="Auto Line Feeding API",
        description="Core microservice responsible for powering all main services to CIAL system",
        docs_url="/core-docs",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(
        assembly_router,
        prefix="/assembly",
        tags=["assembly-line"]
    )

    app.include_router(
        consumption_router,
        prefix="/consumption",
        tags=["consumption"]
    )

    app.include_router(
        forecast_router,
        prefix="/forecast",
        tags=["forecast"]
    )

    app.include_router(
        requests_builder_router,
        prefix="/requests-builder",
        tags=["requests-builder"]
    )

    app.include_router(
        requests_checker_router,
        prefix="/requests-checker",
        tags=["requests-checker"]
    )

    app.include_router(
        requests_closure_router,
        prefix="/requests-closure",
        tags=["requests-closure"]
    )

    app.include_router(
        sap_manager_router,
        prefix="/sap-manager",
        tags=["sap-manager"]
    )

    return app


app = create_app()


if __name__ == "__main__":
    log.info("Starting Uvicorn server with reload support")
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8003,
        reload=True
    )