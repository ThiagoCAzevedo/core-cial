from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from .routes import *


app = FastAPI(
    title="Auto Line Feeding", 
    docs_url="/alf-doc",
    description="Backend API's for Auto Line Feeding System"
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


app.include_router(assembly_router, prefix="/assembly", tags=["assembly"])
app.include_router(forecast_router, prefix="/forecast", tags=["forecast"])
app.include_router(consumption_router, prefix="/consumption", tags=["consumption"])
app.include_router(request_router, prefix="/requests-builder", tags=["request-builder"])
app.include_router(sap_router, prefix="/sap-manager", tags=["sap-manager"])
app.include_router(sp02_router, prefix="/requests-checker/sp02", tags=["requests-checker"])
app.include_router(lt22_router, prefix="/requests-checker/lt22", tags=["requests-checker"])
app.include_router(pkmc_router, prefix="/static/pkmc", tags=["static"])
app.include_router(pk05_router, prefix="/static/pk05", tags=["static"])
app.include_router(static_files_router, prefix="/static/files", tags=["static"])

