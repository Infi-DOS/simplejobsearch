from __future__ import annotations

import pytest

from jobsimplesearch.pipeline.post_ai import classify_post_ai, derive_ai_data_hybrid


def post_rule(rule_id, key, field, operator, value, action, category=None):
    return {
        "rule_id": rule_id,
        "rule_key": key,
        "field_name": field,
        "operator": operator,
        "match_value": value,
        "action": action,
        "review_category": category,
    }


RULES = [
    post_rule(1, "low-confidence", "extraction_confidence", "lt", "0.70", "REVIEW", "low_ai_confidence"),
    post_rule(2, "student", "student_status_required", "is_true", None, "REJECT"),
    post_rule(3, "experience", "minimum_years_experience", "gte", "3", "REJECT"),
    post_rule(4, "phd", "phd_required", "is_true", None, "REJECT"),
    post_rule(5, "senior", "seniority", "in", "senior,staff,principal,lead,manager,director,executive", "REJECT"),
    post_rule(6, "dutch", "languages_required", "contains", "dutch", "REJECT"),
    post_rule(7, "german", "languages_required", "contains", "german", "REJECT"),
    post_rule(8, "other-required", "other_required_language_present", "is_true", None, "REVIEW", "language_requirement"),
    post_rule(9, "preferred-language", "non_english_preferred_language_present", "is_true", None, "REVIEW", "language_requirement"),
    post_rule(10, "bonus-language", "non_english_bonus_language_present", "is_true", None, "REVIEW", "language_requirement"),
    post_rule(11, "hybrid", "ai_data_engineering_hybrid", "is_true", None, "SHORTLIST"),
    post_rule(12, "data-engineering", "role_family", "equals", "data_engineering", "REJECT"),
    post_rule(13, "target", "role_families", "in", "data_science,machine_learning,ai_engineering,computer_vision,deep_learning", "SHORTLIST"),
]


def facts(**overrides):
    role_families = overrides.pop("role_families", ["machine_learning"])
    required = overrides.pop("languages_required", [])
    preferred = overrides.pop("languages_preferred", [])
    bonus = overrides.pop("languages_bonus", [])
    result = {
        "human_decision": None,
        "extraction_confidence": 0.95,
        "student_status_required": False,
        "minimum_years_experience": None,
        "preferred_years_experience": None,
        "phd_required": False,
        "phd_preferred": False,
        "seniority": "entry",
        "languages_required": required,
        "languages_preferred": preferred,
        "languages_bonus": bonus,
        "other_required_language_present": any(x not in {"english", "dutch", "german"} for x in required),
        "non_english_preferred_language_present": any(x != "english" for x in preferred),
        "non_english_bonus_language_present": any(x != "english" for x in bonus),
        "role_family": role_families[0],
        "role_families": role_families,
        "ai_data_engineering_hybrid": derive_ai_data_hybrid(role_families),
    }
    result.update(overrides)
    return result


def classify(**overrides):
    return classify_post_ai(facts(**overrides), RULES).post_ai_status


def test_ai_data_engineering_hybrid_is_not_rejected_as_pure_data_engineering():
    assert classify(role_families=["machine_learning", "data_engineering"]) == "SHORTLIST"
    assert classify(role_families=["data_engineering"]) == "REJECT"


def test_mandatory_three_years_rejects_but_preferred_does_not():
    assert classify(minimum_years_experience=3) == "REJECT"
    assert classify(preferred_years_experience=5) == "SHORTLIST"


@pytest.mark.parametrize("language", ["dutch", "german"])
def test_dutch_and_german_required_reject(language):
    assert classify(languages_required=["english", language]) == "REJECT"


@pytest.mark.parametrize("kind", ["preferred", "bonus"])
@pytest.mark.parametrize("language", ["dutch", "german"])
def test_dutch_and_german_nonmandatory_are_review(kind, language):
    assert classify(**{f"languages_{kind}": [language]}) == "REVIEW"


@pytest.mark.parametrize("required", [[], ["english"]])
def test_no_language_or_english_only_can_continue(required):
    assert classify(languages_required=required) == "SHORTLIST"


def test_mandatory_phd_rejects_but_preferred_phd_does_not():
    assert classify(phd_required=True) == "REJECT"
    assert classify(phd_preferred=True) == "SHORTLIST"


def test_student_status_required_rejects():
    assert classify(student_status_required=True) == "REJECT"


def test_human_exclude_is_authoritative():
    assert classify(human_decision="EXCLUDE") == "REJECT"

