"""Reusable PRE_DESCRIPTION rule services.

The tuned classifier remains database-driven in the established rule engine.
Normal discovery calls it only for newly observed LinkedIn IDs.
"""

from rule_engine import (
    PreClassification,
    classify_pre_description,
    load_category_defaults,
    load_pre_rules,
)

__all__ = [
    "PreClassification",
    "classify_pre_description",
    "load_category_defaults",
    "load_pre_rules",
]

