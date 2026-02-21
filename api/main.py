from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.health import router as health_router
from api.routes.config import router as config_router
from api.routes.insights import router as insights_router
from api.routes.agents import router as agents_router
from api.routes.reports import router as reports_router
from api.routes.ops import router as ops_router
from api.routes.search import router as search_router
from api.routes.compliance import router as compliance_router
from core import db
from core.config import get_settings

settings = get_settings()

cors_origins = list(settings.cors_origins)
if "https://tickeragent.fahimulhaque.org" not in cors_origins:
    cors_origins.append("https://tickeragent.fahimulhaque.org")

app = FastAPI(title="TickerAgent API", version="0.3.0-phase3", root_path="/tickeragent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    db.init_db()


app.include_router(health_router)
app.include_router(config_router)
app.include_router(insights_router)
app.include_router(agents_router)
app.include_router(reports_router)
app.include_router(ops_router)
app.include_router(search_router)
app.include_router(compliance_router)
