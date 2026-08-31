from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Any


# =============================================================================
# DATA TYPES
# =============================================================================

@dataclass(frozen=True)
class PreClassification:
    classifier_status: str
    review_category: str | None
    review_reason: str
    suggested_action: str
    matched_rule_id: int | None
    matched_rule_key: str | None


@dataclass(frozen=True)
class MetadataClassification:
    metadata_gate_status: str
    reason: str
    matched_rule_id: int | None
    matched_rule_key: str | None


@dataclass(frozen=True)
class PostClassification:
    post_ai_status: str
    review_category: str | None
    reason: str
    matched_rule_id: int | None
    matched_rule_key: str | None


# =============================================================================
# NORMALIZATION / COERCION
# =============================================================================

def normalize_text(value: Any) -> str:
    return str(value or "").strip().casefold()


def parse_list(value: Any) -> list[str]:
    text = str(value or "").strip()

    if not text:
        return []

    parts = re.split(
        r"[\n,;|]+",
        text,
    )

    return [
        item.strip().casefold()
        for item in parts
        if item.strip()
    ]


def to_float(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return float(int(value))

    try:
        return float(
            str(value).strip()
        )
    except (
        TypeError,
        ValueError,
    ):
        return None


def to_bool(value: Any) -> bool | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().casefold()

    if text in {
        "true",
        "1",
        "yes",
        "y",
    }:
        return True

    if text in {
        "false",
        "0",
        "no",
        "n",
    }:
        return False

    return None


# =============================================================================
# DATABASE LOADING
# =============================================================================

def load_category_defaults(
    connection: sqlite3.Connection,
) -> dict[str, dict]:

    rows = connection.execute(
        """
        SELECT
            category_key,
            default_action,
            enabled

        FROM review_categories
        """
    ).fetchall()

    return {
        row["category_key"]: {
            "default_action":
                row["default_action"],

            "enabled":
                bool(
                    row["enabled"]
                ),
        }
        for row in rows
    }


def load_pre_rules(
    connection: sqlite3.Connection,
) -> list[dict]:

    rows = connection.execute(
        """
        SELECT
            rule_id,
            rule_key,
            rule_name,
            field_name,
            operator,
            match_value,
            action,
            review_category,
            priority,
            enabled,
            source,
            notes

        FROM pipeline_rules

        WHERE
            stage = 'PRE_DESCRIPTION'
            AND enabled = 1

        ORDER BY
            priority,
            rule_id
        """
    ).fetchall()

    return [
        dict(row)
        for row in rows
    ]


def load_metadata_rules(
    connection: sqlite3.Connection,
) -> list[dict]:

    rows = connection.execute(
        """
        SELECT
            rule_id,
            rule_key,
            rule_name,
            field_name,
            operator,
            match_value,
            action,
            review_category,
            priority,
            enabled,
            source,
            notes

        FROM pipeline_rules

        WHERE
            stage = 'METADATA_GATE'
            AND enabled = 1

        ORDER BY
            priority,
            rule_id
        """
    ).fetchall()

    return [
        dict(row)
        for row in rows
    ]


def load_post_rules(
    connection: sqlite3.Connection,
) -> list[dict]:

    rows = connection.execute(
        """
        SELECT
            rule_id,
            rule_key,
            rule_name,
            field_name,
            operator,
            match_value,
            action,
            review_category,
            priority,
            enabled,
            source,
            notes

        FROM pipeline_rules

        WHERE
            stage = 'POST_AI'
            AND enabled = 1

        ORDER BY
            priority,
            rule_id
        """
    ).fetchall()

    return [
        dict(row)
        for row in rows
    ]


# =============================================================================
# RULE MATCHING
# =============================================================================

def rule_matches(
    rule: dict,
    job: dict,
) -> bool:

    field_name = rule[
        "field_name"
    ]

    operator = rule[
        "operator"
    ]

    match_value = rule[
        "match_value"
    ]

    value = job.get(
        field_name
    )

    # Missing/unknown data should not accidentally satisfy negative rules.
    if value is None:
        return False

    # -------------------------------------------------------------------------
    # Boolean operators
    # -------------------------------------------------------------------------

    if operator == "is_true":
        parsed = to_bool(
            value
        )
        return parsed is True

    if operator == "is_false":
        parsed = to_bool(
            value
        )
        return parsed is False

    # -------------------------------------------------------------------------
    # Numeric comparisons
    # -------------------------------------------------------------------------

    if operator in {
        "gt",
        "gte",
        "lt",
        "lte",
    }:

        left = to_float(
            value
        )

        right = to_float(
            match_value
        )

        if (
            left is None
            or right is None
        ):
            return False

        if operator == "gt":
            return left > right

        if operator == "gte":
            return left >= right

        if operator == "lt":
            return left < right

        if operator == "lte":
            return left <= right

    # -------------------------------------------------------------------------
    # Text / list operators
    #
    # POST_AI fields such as languages_required and technical_skills_required
    # are lists. Scalar PRE_DESCRIPTION behavior remains unchanged.
    # -------------------------------------------------------------------------

    right = normalize_text(
        match_value
    )

    if isinstance(
        value,
        (list, tuple, set),
    ):
        left_values = [
            normalize_text(item)
            for item in value
            if normalize_text(item)
        ]
    else:
        left_values = [
            normalize_text(value)
        ]

    left = (
        left_values[0]
        if len(left_values) == 1
        else " | ".join(left_values)
    )

    if operator == "equals":
        return (
            len(left_values) == 1
            and left_values[0] == right
        )

    if operator == "not_equals":
        return not (
            len(left_values) == 1
            and left_values[0] == right
        )

    if operator == "contains":
        return (
            bool(right)
            and any(
                right in item
                for item in left_values
            )
        )

    if operator == "not_contains":
        return (
            bool(right)
            and all(
                right not in item
                for item in left_values
            )
        )

    if operator == "regex":

        if not match_value:
            return False

        try:
            pattern = re.compile(
                str(match_value),
                flags=re.IGNORECASE,
            )

            return any(
                bool(
                    pattern.search(
                        str(item)
                    )
                )
                for item in (
                    value
                    if isinstance(
                        value,
                        (list, tuple, set),
                    )
                    else [value]
                )
            )

        except re.error:
            return False

    if operator in {
        "in",
        "not_in",
    }:

        options = parse_list(
            match_value
        )

        if not options:
            return False

        matched = any(
            item in options
            for item in left_values
        )

        return (
            matched
            if operator == "in"
            else not matched
        )

    return False


def is_target_research_role(
    job: dict,
) -> bool:
    """
    The special AUTO_KEEP promotion applies only to Research Scientist /
    Research Engineer style titles.

    It must NOT promote PhD, EngD, Professor, generic researcher, or other
    academic roles merely because they also contain AI/ML/CV/DL terminology.
    """
    title = str(
        job.get("title")
        or ""
    )

    return bool(
        re.search(
            (
                r"\bresearch\b"
                r".{0,45}"
                r"\b(?:scientist|engineer)\b"
                r"|"
                r"\b(?:scientist|engineer)\b"
                r".{0,45}"
                r"\bresearch\b"
            ),
            title,
            flags=re.IGNORECASE,
        )
    )


# =============================================================================
# CLASSIFICATION HELPERS
# =============================================================================

def category_is_enabled(
    category_defaults: dict[str, dict],
    category: str | None,
) -> bool:

    if not category:
        return True

    info = category_defaults.get(
        category
    )

    if info is None:
        return True

    return bool(
        info["enabled"]
    )


def suggested_for_category(
    category_defaults: dict[str, dict],
    category: str | None,
) -> str:

    if not category:
        return "REVIEW"

    info = category_defaults.get(
        category
    )

    if not info:
        return "REVIEW"

    if not info["enabled"]:
        return "REVIEW"

    action = str(
        info[
            "default_action"
        ]
        or "REVIEW"
    ).upper()

    if action not in {
        "KEEP",
        "REVIEW",
        "EXCLUDE",
    }:
        return "REVIEW"

    return action


def result_from_rule(
    rule: dict,
    category_defaults: dict[str, dict],
) -> PreClassification:

    action = str(
        rule["action"]
        or ""
    ).upper()

    category = (
        str(
            rule[
                "review_category"
            ]
            or ""
        ).strip()
        or None
    )

    if action == "AUTO_KEEP":

        return PreClassification(
            classifier_status="AUTO_KEEP",
            review_category=None,
            review_reason=(
                "pipeline_rule:"
                + str(
                    rule[
                        "rule_key"
                    ]
                )
            ),
            suggested_action="KEEP",
            matched_rule_id=int(
                rule[
                    "rule_id"
                ]
            ),
            matched_rule_key=str(
                rule[
                    "rule_key"
                ]
            ),
        )

    if action == "AUTO_EXCLUDE":

        return PreClassification(
            classifier_status="AUTO_EXCLUDE",
            review_category=category,
            review_reason=(
                "pipeline_rule:"
                + str(
                    rule[
                        "rule_key"
                    ]
                )
            ),
            suggested_action="EXCLUDE",
            matched_rule_id=int(
                rule[
                    "rule_id"
                ]
            ),
            matched_rule_key=str(
                rule[
                    "rule_key"
                ]
            ),
        )

    if action == "REVIEW":

        return PreClassification(
            classifier_status="REVIEW",
            review_category=(
                category
                or "other_ambiguous"
            ),
            review_reason=(
                "pipeline_rule:"
                + str(
                    rule[
                        "rule_key"
                    ]
                )
            ),
            suggested_action=(
                suggested_for_category(
                    category_defaults,
                    category
                    or "other_ambiguous",
                )
            ),
            matched_rule_id=int(
                rule[
                    "rule_id"
                ]
            ),
            matched_rule_key=str(
                rule[
                    "rule_key"
                ]
            ),
        )

    raise ValueError(
        f"Unsupported PRE_DESCRIPTION action: {action}"
    )


# =============================================================================
# LEGACY-COMPATIBLE PRE-DESCRIPTION CLASSIFIER
#
# This intentionally preserves the ordering model used by the tuned collector:
#
#   1. early priority rules (< 200)
#   2. clear AUTO_KEEP rules
#   3. Research Scientist/Engineer + explicit target-tech composite
#   4. remaining rules
#   5. other_ambiguous fallback
#
# The composite uses DATABASE patterns. It does not hard-code research/AI regex.
# =============================================================================

def classify_pre_description(
    job: dict,
    rules: list[dict],
    category_defaults: dict[str, dict],
) -> PreClassification:

    # Ignore REVIEW rules whose target category has been disabled.
    active_rules = [
        rule
        for rule in rules
        if (
            str(
                rule[
                    "action"
                ]
            ).upper()
            != "REVIEW"
            or category_is_enabled(
                category_defaults,
                rule[
                    "review_category"
                ],
            )
        )
    ]

    early_rules = [
        rule
        for rule in active_rules
        if int(
            rule[
                "priority"
            ]
            or 1000
        ) < 200
    ]

    auto_keep_rules = [
        rule
        for rule in active_rules
        if str(
            rule[
                "action"
            ]
        ).upper()
        == "AUTO_KEEP"
    ]

    research_rules = [
        rule
        for rule in active_rules
        if str(
            rule[
                "review_category"
            ]
            or ""
        )
        == "research"
    ]

    target_tech_rules = [
        rule
        for rule in active_rules
        if str(
            rule[
                "review_category"
            ]
            or ""
        )
        == "emerging_ai_roles"
    ]

    # -------------------------------------------------------------------------
    # 1. Hard exclusions / professional-family review overrides
    # -------------------------------------------------------------------------

    for rule in early_rules:

        if rule_matches(
            rule,
            job,
        ):

            return result_from_rule(
                rule,
                category_defaults,
            )

    # -------------------------------------------------------------------------
    # 2. Clear target roles
    # -------------------------------------------------------------------------

    for rule in auto_keep_rules:

        if rule_matches(
            rule,
            job,
        ):

            return result_from_rule(
                rule,
                category_defaults,
            )

    # -------------------------------------------------------------------------
    # 3. Research Scientist/Engineer + explicit target technology signal
    # -------------------------------------------------------------------------

    research_match = next(
        (
            rule
            for rule in research_rules
            if rule_matches(
                rule,
                job,
            )
        ),
        None,
    )

    target_match = next(
        (
            rule
            for rule in target_tech_rules
            if rule_matches(
                rule,
                job,
            )
        ),
        None,
    )

    if (
        research_match is not None
        and target_match is not None
        and is_target_research_role(
            job
        )
    ):

        return PreClassification(
            classifier_status="AUTO_KEEP",
            review_category=None,
            review_reason=(
                "pipeline_composite:"
                + str(
                    research_match[
                        "rule_key"
                    ]
                )
                + "+"
                + str(
                    target_match[
                        "rule_key"
                    ]
                )
            ),
            suggested_action="KEEP",
            matched_rule_id=int(
                research_match[
                    "rule_id"
                ]
            ),
            matched_rule_key=(
                str(
                    research_match[
                        "rule_key"
                    ]
                )
                + "+"
                + str(
                    target_match[
                        "rule_key"
                    ]
                )
            ),
        )

    # -------------------------------------------------------------------------
    # 4. Everything else in DB priority order
    # -------------------------------------------------------------------------

    early_ids = {
        int(
            rule[
                "rule_id"
            ]
        )
        for rule in early_rules
    }

    auto_keep_ids = {
        int(
            rule[
                "rule_id"
            ]
        )
        for rule in auto_keep_rules
    }

    for rule in active_rules:

        rule_id = int(
            rule[
                "rule_id"
            ]
        )

        if (
            rule_id in early_ids
            or rule_id in auto_keep_ids
        ):
            continue

        if rule_matches(
            rule,
            job,
        ):

            return result_from_rule(
                rule,
                category_defaults,
            )

    # -------------------------------------------------------------------------
    # 5. Safe fallback
    # -------------------------------------------------------------------------

    return PreClassification(
        classifier_status="REVIEW",
        review_category="other_ambiguous",
        review_reason="no_matching_pipeline_rule",
        suggested_action=(
            suggested_for_category(
                category_defaults,
                "other_ambiguous",
            )
        ),
        matched_rule_id=None,
        matched_rule_key=None,
    )


# =============================================================================
# METADATA-GATE CLASSIFIER
#
# First matching enabled metadata rule wins. Safe fallback = PASS.
# The caller is required to supply metadata only; no description is needed.
# =============================================================================

def classify_metadata_gate(
    job: dict,
    rules: list[dict],
) -> MetadataClassification:

    human_decision = str(
        job.get("human_decision")
        or ""
    ).strip().upper()

    classifier_status = str(
        job.get("classifier_status")
        or ""
    ).strip().upper()

    if human_decision == "EXCLUDE":
        return MetadataClassification(
            metadata_gate_status="REJECT",
            reason="human_exclude",
            matched_rule_id=None,
            matched_rule_key=None,
        )

    if (
        classifier_status == "AUTO_EXCLUDE"
        and human_decision != "KEEP"
    ):
        return MetadataClassification(
            metadata_gate_status="REJECT",
            reason="current_pre_description_auto_exclude",
            matched_rule_id=None,
            matched_rule_key=None,
        )

    for rule in rules:

        if not rule_matches(
            rule,
            job,
        ):
            continue

        action = str(
            rule.get("action")
            or ""
        ).strip().upper()

        if action not in {
            "PASS",
            "REJECT",
        }:
            continue

        return MetadataClassification(
            metadata_gate_status=action,
            reason=(
                "pipeline_rule:"
                + str(
                    rule.get("rule_key")
                )
            ),
            matched_rule_id=(
                int(rule["rule_id"])
                if rule.get("rule_id") is not None
                else None
            ),
            matched_rule_key=(
                str(rule.get("rule_key"))
                if rule.get("rule_key") is not None
                else None
            ),
        )

    return MetadataClassification(
        metadata_gate_status="PASS",
        reason="no_matching_metadata_exclusion",
        matched_rule_id=None,
        matched_rule_key=None,
    )


# =============================================================================
# POST-AI CLASSIFIER
#
# Enabled rules are evaluated in ascending priority order. First match wins.
# This deliberately makes rule ordering visible/editable in pipeline_rules.
# =============================================================================

def classify_post_ai(
    job: dict,
    rules: list[dict],
) -> PostClassification:

    # Explicit human EXCLUDE is authoritative. Human KEEP at the pre-fetch
    # stage is not a final shortlist decision, so normal POST_AI rules still
    # evaluate it.
    human_decision = str(
        job.get(
            "human_decision"
        )
        or ""
    ).strip().upper()

    if human_decision == "EXCLUDE":
        return PostClassification(
            post_ai_status="REJECT",
            review_category=None,
            reason="human_exclude",
            matched_rule_id=None,
            matched_rule_key=None,
        )

    for rule in rules:

        if not rule_matches(
            rule,
            job,
        ):
            continue

        action = str(
            rule.get(
                "action"
            )
            or ""
        ).strip().upper()

        if action not in {
            "SHORTLIST",
            "REVIEW",
            "REJECT",
        }:
            continue

        category = (
            str(
                rule.get(
                    "review_category"
                )
                or ""
            ).strip()
            or None
        )

        if (
            action == "REVIEW"
            and category is None
        ):
            category = (
                "other_ambiguous"
            )

        return PostClassification(
            post_ai_status=action,
            review_category=(
                category
                if action == "REVIEW"
                else None
            ),
            reason=(
                "pipeline_rule:"
                + str(
                    rule.get(
                        "rule_key"
                    )
                )
            ),
            matched_rule_id=(
                int(
                    rule[
                        "rule_id"
                    ]
                )
                if rule.get(
                    "rule_id"
                ) is not None
                else None
            ),
            matched_rule_key=(
                str(
                    rule.get(
                        "rule_key"
                    )
                )
                if rule.get(
                    "rule_key"
                ) is not None
                else None
            ),
        )

    # Safe fallback: unexplained extracted jobs never become SHORTLIST or
    # REJECT merely because the rule table has a gap.
    return PostClassification(
        post_ai_status="REVIEW",
        review_category="other_ambiguous",
        reason="no_matching_post_ai_rule",
        matched_rule_id=None,
        matched_rule_key=None,
    )
