"""Ensure operational logging in tests can never target the user's live DB."""
import pytest

from simplejobsearch import config


@pytest.fixture(autouse=True)
def isolate_default_database(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(tmp_path / "default-test.db"))
    config.reset_settings_cache()
    yield
    config.reset_settings_cache()
