"""Application logger configuration for MLHandler.

Provides a configured logger that writes INFO and ERROR level logs to
`logs/app.log` and prints to console. Use `get_logger()` to retrieve
the configured logger in other modules.
"""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path


LOG_DIR = Path(__file__).resolve().parents[3] / "logs"
LOG_FILE = LOG_DIR / "app.log"


def _ensure_log_dir() -> None:
	try:
		LOG_DIR.mkdir(parents=True, exist_ok=True)
	except Exception:
		# best-effort: if cannot create, handlers will raise later
		pass


def _configure_logger(name: str = "mlhandler") -> logging.Logger:
	_ensure_log_dir()

	logger = logging.getLogger(name)
	if logger.handlers:
		# already configured
		return logger

	logger.setLevel(logging.INFO)

	# File handler with rotation
	try:
		fh = logging.handlers.RotatingFileHandler(str(LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
		fh.setLevel(logging.INFO)
	except Exception:
		fh = logging.StreamHandler()
		fh.setLevel(logging.INFO)

	# Console handler for errors and above
	ch = logging.StreamHandler()
	ch.setLevel(logging.ERROR)

	fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
	fh.setFormatter(fmt)
	ch.setFormatter(fmt)

	logger.addHandler(fh)
	logger.addHandler(ch)

	# Avoid duplicate logs if root logger is configured elsewhere
	logger.propagate = False
	return logger


_logger = _configure_logger()


def get_logger(name: str | None = None) -> logging.Logger:
	"""Return a configured logger. If `name` is provided, return a child logger."""
	if name:
		return _logger.getChild(name)
	return _logger


__all__ = ["get_logger"]
