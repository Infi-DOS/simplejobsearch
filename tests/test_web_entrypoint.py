from __future__ import annotations

import asyncio
import importlib
import sys
from types import SimpleNamespace

from simplejobsearch import cli, config


def test_packaged_web_entrypoint_uses_root_callable_without_import_side_effect(
    monkeypatch,
):
    import nicegui

    label_calls = []
    monkeypatch.setattr(
        nicegui.ui,
        "label",
        lambda *_args, **_kwargs: label_calls.append(True),
    )
    sys.modules.pop("simplejobsearch.ui.app", None)
    sys.modules.pop("simplejobsearch.ui.views", None)

    web_app = importlib.import_module("simplejobsearch.ui.app")

    assert label_calls == []

    captured = {}
    monkeypatch.setattr(
        web_app.ui,
        "run",
        lambda **kwargs: captured.update(kwargs),
    )
    monkeypatch.setenv("WEB_HOST", "127.0.0.1")
    monkeypatch.setenv("WEB_PORT", "5099")
    config.reset_settings_cache()

    web_app.run_web()

    assert captured["root"] is web_app.build_ui
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 5099
    assert captured["reload"] is False
    config.reset_settings_cache()


def test_cli_web_command_invokes_packaged_run_web(monkeypatch):
    from simplejobsearch.ui import app as web_app

    calls = []
    monkeypatch.setattr(web_app, "run_web", lambda: calls.append("web"))

    assert cli.main(["web"]) == 0
    assert calls == ["web"]


def test_public_paths_select_the_expected_initial_tab():
    from simplejobsearch.ui import views

    review_request = SimpleNamespace(url=SimpleNamespace(path="/review"))
    results_request = SimpleNamespace(url=SimpleNamespace(path="/results"))
    root_request = SimpleNamespace(url=SimpleNamespace(path="/"))

    assert views._initial_ui_view(review_request) == "review"
    assert views._initial_ui_view(results_request) == "results"
    assert views._initial_ui_view(root_request) == "review"
    assert views._initial_ui_view(None) == "review"


def test_ai_extraction_view_uses_the_explicit_client_grid(monkeypatch):
    from simplejobsearch.ui import views

    class FakeGrid:
        def __init__(self, rows):
            self.rows = rows
            self.calls = 0

        async def get_selected_rows(self):
            self.calls += 1
            return self.rows

    stale_global_grid = FakeGrid([{"job_id": "stale"}])
    current_client_grid = FakeGrid([{"job_id": "current"}])
    displayed = []

    monkeypatch.setattr(views, "ai_extracted_grid", stale_global_grid)
    monkeypatch.setattr(
        views,
        "show_job_detail_dialog",
        lambda row: displayed.append(row["job_id"]),
    )

    asyncio.run(
        views.show_selected_ai_extracted_job(
            current_client_grid
        )
    )

    assert current_client_grid.calls == 1
    assert stale_global_grid.calls == 0
    assert displayed == ["current"]


def test_review_decision_uses_the_explicit_client_grid(monkeypatch):
    from simplejobsearch.ui import views

    class FakeGrid:
        def __init__(self, rows):
            self.rows = rows
            self.calls = 0

        async def get_selected_rows(self):
            self.calls += 1
            return self.rows

    stale_global_grid = FakeGrid([])
    current_client_grid = FakeGrid([{"job_ids_json": '["current"]'}])
    saved = []

    monkeypatch.setattr(views, "grid", stale_global_grid)
    monkeypatch.setattr(
        views,
        "save_review_decisions",
        lambda rows, decision: saved.append((rows, decision))
        or {"saved_jobs": 1, "groups": 1},
    )
    monkeypatch.setattr(views.ui, "notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(views, "refresh_review", lambda: None)

    asyncio.run(views.apply_decision("EXCLUDE", current_client_grid))

    assert current_client_grid.calls == 1
    assert stale_global_grid.calls == 0
    assert saved == [([{"job_ids_json": '["current"]'}], "EXCLUDE")]


def test_grid_columns_follow_workflow_order():
    from simplejobsearch.ui import views

    def visible_fields(options):
        return [
            column["field"]
            for column in options["columnDefs"]
            if not column.get("hide")
        ]

    assert visible_fields(views.make_grid_options([])) == [
        "suggested",
        "title",
        "company",
        "category",
        "locations",
        "posted",
        "listings",
        "phd",
    ]
    assert visible_fields(views.prefetch_grid_options([])) == [
        "decision",
        "title",
        "company",
        "classifier_status",
        "human_decision",
        "category",
        "locations",
        "posted",
        "listings",
        "details_status",
        "phd",
    ]
    assert visible_fields(views.fetched_grid_options([])) == [
        "title",
        "company",
        "metadata_gate_status",
        "ai_status",
        "location",
        "job_type",
        "job_level",
        "job_function",
        "ai_role_family",
        "ai_seniority",
        "ai_minimum_years_experience",
        "description_preview",
        "description_chars",
        "fetched_at",
    ]
    assert visible_fields(views.ai_extracted_grid_options([])) == [
        "post_ai_status",
        "title",
        "company",
        "location",
        "ai_role_family",
        "ai_seniority",
        "ai_minimum_years_experience",
        "ai_student_status_required",
        "ai_minimum_degree_level",
        "ai_phd_required",
        "post_ai_reason",
        "ai_languages_display",
        "ai_role_families_display",
        "ai_data_engineering_hybrid",
        "ai_preferred_years_experience",
        "ai_languages_preferred_display",
        "ai_languages_bonus_display",
        "ai_extraction_confidence",
    ]
    assert visible_fields(views.category_grid_options([])) == [
        "enabled",
        "sort_order",
        "display_name",
        "default_action",
        "category_key",
        "updated_at",
    ]
    assert visible_fields(views.pipeline_rule_grid_options([])) == [
        "enabled",
        "priority",
        "rule_name",
        "field_name",
        "operator",
        "match_value",
        "action",
        "review_category",
        "notes",
        "source",
    ]


def test_workflow_grids_unpin_columns_on_mobile_clients():
    from simplejobsearch.ui import views

    for options in (
        views.make_grid_options([]),
        views.prefetch_grid_options([]),
        views.fetched_grid_options([]),
        views.ai_extracted_grid_options([]),
    ):
        assert "max-width: 700px" in options[":onGridReady"]
        assert "pinned: null" in options[":onGridReady"]
        assert "clientWidth" in options[":onGridSizeChanged"]
