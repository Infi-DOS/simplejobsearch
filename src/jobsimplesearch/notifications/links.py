from __future__ import annotations


def public_url(public_base_url: str | None, path: str) -> str | None:
    if not public_base_url:
        return None
    base = public_base_url.rstrip("/")
    suffix = path.strip("/")
    return f"{base}/{suffix}" if suffix else base


def review_url(public_base_url: str | None) -> str | None:
    return public_url(public_base_url, "/review")


def results_url(public_base_url: str | None) -> str | None:
    return public_url(public_base_url, "/results")
