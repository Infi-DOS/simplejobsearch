from __future__ import annotations

from jobsimplesearch import config
from jobsimplesearch.notifications.links import public_url, results_url, review_url


def test_public_base_url_is_normalized(monkeypatch):
    monkeypatch.setenv(
        "PUBLIC_BASE_URL",
        "  https://jobs-example.ngrok-free.app///  ",
    )
    config.reset_settings_cache()

    assert (
        config.get_settings().web.public_base_url
        == "https://jobs-example.ngrok-free.app"
    )

    config.reset_settings_cache()


def test_missing_public_base_url_is_safe(monkeypatch):
    monkeypatch.setenv("PUBLIC_BASE_URL", "")
    config.reset_settings_cache()

    assert config.get_settings().web.public_base_url is None
    assert public_url(None, "/review") is None

    config.reset_settings_cache()


def test_review_and_results_urls_are_generated_without_double_slashes():
    base = "https://jobs-example.ngrok-free.app/"

    assert review_url(base) == "https://jobs-example.ngrok-free.app/review"
    assert results_url(base) == "https://jobs-example.ngrok-free.app/results"
