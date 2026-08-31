from __future__ import annotations

from nicegui import ui

from ..config import get_settings
from .views import build_ui


def run_web() -> None:
    settings = get_settings()
    ui.run(
        root=build_ui,
        host=settings.web.host,
        port=settings.web.port,
        title="Job Simple Search",
        reload=False,
    )


main = run_web


if __name__ == "__main__":
    run_web()
