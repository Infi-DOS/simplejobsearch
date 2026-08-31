from __future__ import annotations

import sqlite3

import pytest

from jobsimplesearch.pipeline.metadata_gate import (
    METADATA_FIELDS,
    classify_metadata_gate,
    load_metadata_job,
)


def rule(rule_id: int, field: str, operator: str, value: str) -> dict:
    return {
        "rule_id": rule_id,
        "rule_key": f"metadata-{rule_id}",
        "field_name": field,
        "operator": operator,
        "match_value": value,
        "action": "REJECT",
    }


RULES = [
    rule(1, "job_type", "contains", "INTERNSHIP"),
    rule(2, "job_level", "equals", "Director"),
    rule(3, "job_level", "equals", "Executive"),
]


@pytest.mark.parametrize(
    ("job_type", "job_level", "expected"),
    [
        ("FULL_TIME, INTERNSHIP", "Entry level", "REJECT"),
        ("FULL_TIME", "Director", "REJECT"),
        ("FULL_TIME", "Mid-Senior level", "PASS"),
    ],
)
def test_conservative_metadata_rules(job_type, job_level, expected):
    result = classify_metadata_gate(
        {
            "job_type": job_type,
            "job_level": job_level,
            "human_decision": None,
            "classifier_status": "AUTO_KEEP",
        },
        RULES,
    )
    assert result.metadata_gate_status == expected


def test_metadata_loader_cannot_read_description():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    columns = ", ".join(f"{name} TEXT" for name in METADATA_FIELDS if name != "job_id")
    connection.execute(f"CREATE TABLE jobs (job_id TEXT PRIMARY KEY, {columns}, description TEXT)")
    connection.execute(
        "INSERT INTO jobs (job_id, title, description) VALUES ('li-1', 'ML Engineer', 'SECRET')"
    )

    def authorizer(action, arg1, arg2, _database, _trigger):
        if action == sqlite3.SQLITE_READ and arg1 == "jobs" and arg2 == "description":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorizer)
    loaded = load_metadata_job(connection, "li-1")
    assert set(loaded) == set(METADATA_FIELDS)
    assert "description" not in loaded

