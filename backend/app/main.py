from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.logger import get_logger
from app.core.config import ALLOWED_ORIGINS
from app.api.routes import router

logger = get_logger("app.main")

app = FastAPI(title="MLHandler API")

app.add_middleware(
	CORSMiddleware,
	allow_origins=ALLOWED_ORIGINS,
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)
app.include_router(router)


@app.on_event("startup")
async def _on_startup() -> None:
	logger.info("MLHandler application starting up")


@app.on_event("shutdown")
async def _on_shutdown() -> None:
	logger.info("MLHandler application shutting down")


@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception):
	# Log full traceback for unexpected exceptions
	logger.exception("Unhandled exception during request %s %s: %s", request.method, request.url, exc)
	return JSONResponse({"detail": "Internal server error"}, status_code=500)


@app.get("/health", tags=["health"])
async def health() -> dict:
	"""Simple health check for readiness/liveness probes."""
	return {"status": "ok"}

