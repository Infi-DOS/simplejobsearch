from __future__ import annotations

import asyncio
import importlib
import json
import sqlite3
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
    final_review_request = SimpleNamespace(url=SimpleNamespace(path="/final-review"))
    post_ai_review_request = SimpleNamespace(
        url=SimpleNamespace(path="/post-ai-review")
    )
    recommended_jobs_request = SimpleNamespace(
        url=SimpleNamespace(path="/recommended-jobs")
    )
    root_request = SimpleNamespace(url=SimpleNamespace(path="/"))

    assert views._initial_ui_view(review_request) == "review"
    assert views._initial_ui_view(results_request) == "post_ai_review"
    assert views._initial_ui_view(final_review_request) == "post_ai_review"
    assert views._initial_ui_view(post_ai_review_request) == "post_ai_review"
    assert views._initial_ui_view(recommended_jobs_request) == "recommended_jobs"
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
        "reason",
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
    assert visible_fields(views.final_review_grid_options([])) == [
        "post_ai_status",
        "post_ai_reason_display",
        "post_ai_review_category_display",
        "human_decision_display",
        "title",
        "company",
        "location",
        "ai_role_family",
        "ai_seniority",
        "ai_minimum_years_experience",
        "ai_languages_display",
        "ai_role_families_display",
        "final_decided_at",
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


def test_review_filter_uses_the_chosen_column_and_is_case_insensitive():
    from simplejobsearch.ui import views

    rows = [
        {
            "title": "Machine Learning Engineer",
            "company": "Example Labs",
            "locations": "Amsterdam",
        },
        {
            "title": "Data Engineer",
            "company": "Northwind AI",
            "locations": "Rotterdam",
        },
    ]

    assert views.filter_review_rows(rows, "company", "NORTHWIND") == [rows[1]]
    assert views.filter_review_rows(rows, "locations", "sterd") == [rows[0]]
    assert views.filter_review_rows(rows, "title", "  ") == rows


def test_review_exclusion_rule_values_are_deduplicated_case_insensitively():
    from simplejobsearch.ui import views

    rows = [
        {"title": "Data Engineer", "company": "Example Labs"},
        {"title": "data engineer", "company": "EXAMPLE LABS"},
        {"title": "ML Engineer", "company": "Other Company"},
    ]

    assert views.review_exclusion_rule_values(rows, "title_equals") == (
        "title",
        "equals",
        ["Data Engineer", "ML Engineer"],
    )
    assert views.review_exclusion_rule_values(rows, "company_equals") == (
        "company",
        "equals",
        ["Example Labs", "Other Company"],
    )


def test_exclude_and_add_review_rules_is_atomic_and_reuses_rules(
    tmp_path,
    monkeypatch,
):
    from simplejobsearch.ui import views

    database_path = tmp_path / "review-rules.db"

    def connect():
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        return connection

    connection = connect()
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            human_decision TEXT,
            human_reviewed_at TEXT,
            review_category TEXT
        );

        CREATE TABLE review_events (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            previous_decision TEXT,
            decision TEXT NOT NULL,
            review_category TEXT,
            source TEXT,
            reviewer TEXT,
            reviewed_at TEXT NOT NULL,
            note TEXT
        );

        CREATE TABLE pipeline_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_key TEXT NOT NULL UNIQUE,
            rule_name TEXT NOT NULL,
            stage TEXT NOT NULL,
            field_name TEXT NOT NULL,
            operator TEXT NOT NULL,
            match_value TEXT,
            action TEXT NOT NULL,
            review_category TEXT,
            priority INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            editable INTEGER NOT NULL,
            source TEXT NOT NULL,
            notes TEXT,
            legacy_filter_rule_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    connection.executemany(
        "INSERT INTO jobs (job_id, review_category) VALUES (?, ?)",
        [("job-1", "general"), ("job-2", "general")],
    )
    connection.commit()
    connection.close()

    monkeypatch.setattr(views, "connect_database", connect)
    selected = [
        {
            "title": "Data Engineer",
            "company": "Example Labs",
            "job_ids_json": json.dumps(["job-1", "job-2"]),
        }
    ]

    first = views.exclude_and_add_review_rules(selected, "title_equals")

    assert first == {
        "saved_jobs": 2,
        "groups": 1,
        "already_reviewed": 0,
        "rules_created": 1,
        "rules_updated": 0,
        "rules_reused": 0,
    }

    second = views.exclude_and_add_review_rules(selected, "title_equals")

    assert second == {
        "saved_jobs": 0,
        "groups": 1,
        "already_reviewed": 2,
        "rules_created": 0,
        "rules_updated": 0,
        "rules_reused": 1,
    }

    connection = connect()
    decisions = connection.execute(
        "SELECT human_decision FROM jobs ORDER BY job_id"
    ).fetchall()
    rule = connection.execute(
        """
        SELECT
            stage,
            field_name,
            operator,
            match_value,
            action,
            priority,
            enabled,
            source
        FROM pipeline_rules
        """
    ).fetchone()
    event_count = connection.execute(
        "SELECT COUNT(*) FROM review_events"
    ).fetchone()[0]
    connection.close()

    assert [row[0] for row in decisions] == ["EXCLUDE", "EXCLUDE"]
    assert dict(rule) == {
        "stage": "PRE_DESCRIPTION",
        "field_name": "title",
        "operator": "equals",
        "match_value": "Data Engineer",
        "action": "AUTO_EXCLUDE",
        "priority": views.REVIEW_EXCLUSION_RULE_PRIORITY,
        "enabled": 1,
        "source": "review_inbox",
    }
    assert event_count == 2


def test_review_exclusion_rule_rolls_back_decisions_when_rule_write_fails(
    tmp_path,
    monkeypatch,
):
    from simplejobsearch.ui import views

    database_path = tmp_path / "review-rule-rollback.db"

    def connect():
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        return connection

    connection = connect()
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            human_decision TEXT,
            human_reviewed_at TEXT,
            review_category TEXT
        );
        CREATE TABLE review_events (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            previous_decision TEXT,
            decision TEXT NOT NULL,
            review_category TEXT,
            source TEXT,
            reviewer TEXT,
            reviewed_at TEXT NOT NULL,
            note TEXT
        );
        CREATE TABLE pipeline_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_key TEXT NOT NULL UNIQUE,
            rule_name TEXT NOT NULL,
            stage TEXT NOT NULL,
            field_name TEXT NOT NULL,
            operator TEXT NOT NULL,
            match_value TEXT,
            action TEXT NOT NULL,
            review_category TEXT,
            priority INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            editable INTEGER NOT NULL,
            source TEXT NOT NULL CHECK (source = 'blocked'),
            notes TEXT,
            legacy_filter_rule_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT INTO jobs (job_id, review_category) VALUES ('job-1', 'general');
        """
    )
    connection.commit()
    connection.close()

    monkeypatch.setattr(views, "connect_database", connect)
    selected = [
        {
            "title": "Data Engineer",
            "company": "Example Labs",
            "job_ids_json": '["job-1"]',
        }
    ]

    try:
        views.exclude_and_add_review_rules(selected, "title_contains")
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("Expected the rule insert to fail")

    connection = connect()
    job = connection.execute(
        "SELECT human_decision FROM jobs WHERE job_id = 'job-1'"
    ).fetchone()
    event_count = connection.execute(
        "SELECT COUNT(*) FROM review_events"
    ).fetchone()[0]
    connection.close()

    assert job[0] is None
    assert event_count == 0


def test_recommended_jobs_loader_uses_only_human_kept_rows(monkeypatch):
    from simplejobsearch.ui import views

    calls = []
    expected = [{"job_id": "kept"}]
    monkeypatch.setattr(
        views,
        "load_final_review_jobs",
        lambda decision, scope: calls.append((decision, scope)) or expected,
    )

    assert views.load_recommended_jobs("All recommended jobs") == expected
    assert calls == [("ACCEPTED", "All recommended jobs")]


def test_recommended_jobs_map_data_groups_supported_locations():
    from simplejobsearch.ui import views

    rows = [
        {
            "job_id": "nl-1",
            "title": "AI Engineer",
            "company": "Example NL",
            "location": "Amsterdam, North Holland, Netherlands",
            "job_url": "https://example.com/nl-1",
        },
        {
            "job_id": "nl-2",
            "title": "ML Engineer",
            "company": "Example NL",
            "location": "Amsterdam, North Holland, Netherlands",
            "job_url": "https://example.com/nl-2",
        },
        {
            "job_id": "ch-1",
            "title": "Data Scientist",
            "company": "Example CH",
            "location": "Zürich, Zurich, Switzerland",
            "job_url": "https://example.com/ch-1",
        },
        {
            "job_id": "ch-2",
            "title": "Machine Learning Engineer",
            "company": "Example CH",
            "location": "Zurich, Switzerland",
            "job_url": "https://example.com/ch-2",
        },
        {
            "job_id": "missing",
            "title": "Unknown location",
            "company": "Example",
            "location": None,
        },
    ]

    data = views.recommended_jobs_map_data(rows)

    assert len(data["Netherlands"]) == 1
    assert data["Netherlands"][0]["coordinates"] == (52.3676, 4.9041)
    assert len(data["Netherlands"][0]["jobs"]) == 2
    assert len(data["Switzerland"]) == 1
    assert data["Switzerland"][0]["coordinates"] == (47.3769, 8.5417)
    assert len(data["Switzerland"][0]["jobs"]) == 2
    assert [row["job_id"] for row in data["unmapped"]] == ["missing"]


def test_job_description_dialog_renders_without_post_ai_rule_notes():
    from nicegui import context
    from simplejobsearch.ui import views

    client = context.client
    existing_ids = set(client.elements)
    views.show_job_detail_dialog(
        {
            "title": "AI Engineer",
            "company": "Example",
            "location": "Amsterdam",
            "job_type": "FULL_TIME",
            "job_level": "",
            "classifier_status": "AUTO_KEEP",
            "metadata_gate_status": "PASS",
            "metadata_gate_reason": "",
            "ai_status": "",
            "description": "Distinct full job description.",
            "job_url": "https://example.test/job",
            "job_url_direct": "",
        }
    )
    elements = [
        element
        for element_id, element in client.elements.items()
        if element_id not in existing_ids
    ]

    markdown = next(
        element for element in elements if element.__class__.__name__ == "Markdown"
    )
    ancestors = []
    parent = markdown.parent_slot.parent
    while parent is not None:
        ancestors.append(parent.__class__.__name__)
        parent_slot = getattr(parent, "parent_slot", None)
        parent = parent_slot.parent if parent_slot is not None else None

    assert markdown.content == "Distinct full job description."
    assert "Card" in ancestors
    assert any(
        element.__class__.__name__ == "Button" and element.text == "Close"
        for element in elements
    )


def test_workflow_grids_unpin_columns_on_mobile_clients():
    from simplejobsearch.ui import views

    for options in (
        views.make_grid_options([]),
        views.prefetch_grid_options([]),
        views.fetched_grid_options([]),
        views.ai_extracted_grid_options([]),
        views.final_review_grid_options([]),
    ):
        assert "max-width: 700px" in options[":onGridReady"]
        assert "pinned: null" in options[":onGridReady"]
        assert "clientWidth" in options[":onGridSizeChanged"]
