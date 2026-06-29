from __future__ import annotations

__all__ = ["create_app"]


def create_app():
    from .app import create_app as app_factory

    return app_factory()
