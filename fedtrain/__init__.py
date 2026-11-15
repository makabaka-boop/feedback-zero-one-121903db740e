"""``fedtrain``: a small federated training protocol with exact arithmetic."""

from .service import Service, close, init, model, open, reset, seal, status

__all__ = [
    "Service",
    "init",
    "open",
    "seal",
    "status",
    "model",
    "close",
    "reset",
]
