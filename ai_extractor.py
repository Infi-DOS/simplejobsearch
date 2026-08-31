from __future__ import annotations

import asyncio
import json
import math
import re
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from google import genai
from google.genai import types
from pydantic import BaseModel, Field, ValidationError

from jobsimplesearch.config import get_settings
from jobsimplesearch.db import connect as shared_connect
from jobsimplesearch.pipeline.retry import (
    is_transient_ai_error,
    retry_delay_seconds,
)
from post_ai_engine import (
    derive_ai_data_hybrid,
    evaluate_and_persist_post_ai,
)

# =============================================================================
# ENV / CONFIG
# =============================================================================

SETTINGS = get_settings()
PROJECT_DIR = SETTINGS.project_root
DATABASE_PATH = SETTINGS.database_path
TIMEZONE = SETTINGS.timezone

# Direct Google GenAI SDK model ID: no "gemini/" LiteLLM prefix.
MODEL = SETTINGS.ai.model

# Gemma 4 supports HIGH or MINIMAL through the Gemini API.
#
# HIGH is the default here because the extraction contains nuanced distinctions
# such as mandatory vs preferred skills and education. This is configurable;
# we should empirically compare HIGH vs MINIMAL on the same labelled jobs.
THINKING_LEVEL = SETTINGS.ai.thinking_level

if THINKING_LEVEL not in {
    "HIGH",
    "MINIMAL",
}:
    raise ValueError(
        "GEMMA_THINKING_LEVEL must be HIGH or MINIMAL."
    )

# Google Gemma 4 model-card recommended sampling settings.
TEMPERATURE = SETTINGS.ai.temperature
TOP_P = SETTINGS.ai.top_p
TOP_K = SETTINGS.ai.top_k

# -------------------------------------------------------------------------
# RATE HANDLER
#
# Google defines TPM as INPUT TOKENS PER MINUTE.
# Therefore output tokens / thinking tokens are NOT reserved against this
# 16K TPM bucket.
#
# The handler enforces:
#   - up to TARGET_RPM request launches in a rolling 60s window
#   - up to EFFECTIVE_TPM estimated input tokens in rolling 60s
#   - up to MAX_CONCURRENCY in-flight generation calls
# -------------------------------------------------------------------------

TARGET_RPM = SETTINGS.ai.target_rpm
MAX_CONCURRENCY = SETTINGS.ai.max_concurrency
PROVIDER_TPM = SETTINGS.ai.provider_tpm
TPM_SAFETY_FACTOR = SETTINGS.ai.tpm_safety_factor

EFFECTIVE_TPM = int(
    PROVIDER_TPM
    * TPM_SAFETY_FACTOR
)

# Official Google guidance says ~4 chars/token for Gemini-family tokenization.
# We intentionally estimate slightly conservatively before launch.
CHARS_PER_TOKEN_ESTIMATE = SETTINGS.ai.chars_per_token_estimate
MAX_ATTEMPTS_PER_JOB = SETTINGS.ai.max_attempts_per_job
MAX_SCHEMA_REPAIR_ATTEMPTS = SETTINGS.ai.max_schema_repair_attempts
AI_RETRY_BASE_SECONDS = SETTINGS.ai.retry_base_seconds
AI_RETRY_MAX_SECONDS = SETTINGS.ai.retry_max_seconds

# 0 = all currently eligible jobs.
MAX_JOBS_PER_RUN = SETTINGS.ai.max_jobs_per_run
AI_PROBE_MODE = SETTINGS.ai.probe_mode

SCHEMA_VERSION = "job_facts_v2"
PROMPT_VERSION = "job_facts_google_genai_v2"


# =============================================================================
# PYDANTIC CONTRACT
# =============================================================================

RoleFamily = Literal[
    "data_science",
    "machine_learning",
    "ai_engineering",
    "computer_vision",
    "deep_learning",
    "research",
    "data_engineering",
    "software_engineering",
    "platform_infrastructure",
    "analyst",
    "consulting",
    "solutions_architect",
    "other",
]

Seniority = Literal[
    "intern",
    "entry",
    "junior",
    "mid",
    "senior",
    "staff",
    "principal",
    "lead",
    "manager",
    "director",
    "executive",
    "unknown",
]

DegreeLevel = Literal[
    "none_specified",
    "vocational",
    "bachelor",
    "master",
    "phd",
    "other",
]

RemoteMode = Literal[
    "onsite",
    "hybrid",
    "remote",
    "unspecified",
]


class EvidenceItem(BaseModel):
    field: str
    evidence: str


class JobFacts(BaseModel):

    # Multi-label role extraction is required because titles such as
    # "Data & AI Engineer" must not collapse into pure Data Engineering.
    primary_role_family: RoleFamily

    role_families: list[
        RoleFamily
    ] = Field(
        min_length=1,
        max_length=3,
    )

    seniority: Seniority

    # Mandatory minimum only.
    minimum_years_experience: (
        float
        | None
    ) = Field(
        default=None,
        ge=0.0,
    )

    experience_requirement_text: (
        str
        | None
    ) = None

    # Preferred/desired years are intentionally separate so they never trigger
    # the >=3 mandatory-experience rejection rule.
    preferred_years_experience: (
        float
        | None
    ) = Field(
        default=None,
        ge=0.0,
    )

    preferred_experience_text: (
        str
        | None
    ) = None

    # Education is decomposed into atomic facts.
    degree_required: bool

    minimum_degree_level: DegreeLevel

    degree_requirement_text: (
        str
        | None
    ) = None

    degree_fields: list[str]

    phd_preferred: bool

    # Some internships/graduate roles require active enrollment even when the
    # title itself does not say "intern".
    student_status_required: bool

    student_requirement_text: (
        str
        | None
    ) = None

    # Language requirement levels are separate.
    # Use canonical English language names: English, Dutch, German, French...
    languages_required: list[str]

    languages_preferred: list[str]

    languages_bonus: list[str]

    language_requirement_notes: (
        str
        | None
    ) = None

    # Technical requirements are also separated mandatory vs optional.
    technical_skills_required: list[str]

    technical_skills_preferred: list[str]

    role_summary: str

    core_responsibilities: list[str]

    remote_mode: RemoteMode

    management_responsibility: bool

    security_clearance_required: bool

    extraction_confidence: float = Field(
        ge=0.0,
        le=1.0,
    )

    evidence: list[
        EvidenceItem
    ]


# =============================================================================
# PROMPT
# =============================================================================

SYSTEM_INSTRUCTION = """
You are a factual job-advertisement information extractor.

Return exactly one JSON object that matches the supplied JSON Schema.
No Markdown.
No code fences.
No commentary.

Use only the supplied title, LinkedIn metadata, and full job description.
Do not decide whether the candidate should apply.
Do not score candidate fit.
Do not use external knowledge about the employer.

The downstream system makes deterministic decisions from your extracted facts,
so REQUIRED vs PREFERRED vs BONUS distinctions are critical.

1. primary_role_family and role_families

primary_role_family is the best single description of the job's main work.

role_families is a multi-label list of substantial CORE job families, maximum
3. Include a family only when its responsibilities are materially part of the
job, not because a tool/keyword is merely mentioned.

Important hybrid example:
A genuine "Data & AI Engineer" that both builds data pipelines/platforms AND
builds/deploys AI/ML systems should have:
  role_families = ["data_engineering", "ai_engineering"]
(or machine_learning where appropriate).

A normal Data Engineer who only supports data used by AI teams should remain:
  role_families = ["data_engineering"].

2. seniority

Use explicit title/level/responsibility wording.
Do not infer "senior" merely because years of experience are requested.
If genuinely unclear, use "unknown".

3. minimum_years_experience

This field is ONLY the mandatory minimum number of relevant/professional years.

Examples:
  "at least 3 years" -> 3
  "3+ years" -> 3
  "3-5 years required" -> 3

If experience is preferred, desired, a plus, or nice-to-have, DO NOT put that
number in minimum_years_experience. Put it in preferred_years_experience.

If no explicit mandatory numeric minimum exists -> null.

Do not output 0 unless the advertisement explicitly says no prior experience
is required / 0 years.

experience_requirement_text should contain a concise source phrase supporting
the mandatory requirement, or null.

4. preferred_years_experience

Extract a numeric preferred/desired experience level only when the job clearly
marks it as preferred/desired/plus rather than mandatory. Otherwise null.

5. Education

degree_required:
true only if a degree is mandatory.

minimum_degree_level:
the minimum mandatory level:
  none_specified / vocational / bachelor / master / phd / other

Examples:
  "Bachelor or Master required" -> bachelor
  "Master or PhD required" -> master
  "PhD required" -> phd
  "PhD preferred" does NOT make minimum_degree_level=phd.

degree_fields:
explicit fields/disciplines, e.g. ["Computer Science", "Mathematics"].

phd_preferred:
true only if a PhD is explicitly preferred/advantageous but not mandatory.

6. student_status_required

true only when being currently enrolled / a current student is a mandatory
eligibility requirement.

Examples that mean true:
  "You must be enrolled at a university"
  "Applicants must currently be students"
  "Enrollment for the duration of the internship is required"

"Students are welcome to apply" does not necessarily mean true.

7. Languages

Use canonical English names for languages regardless of the advertisement
language:
  English, Dutch, German, French, Spanish, etc.
For example, "Nederlands" -> "Dutch", "Deutsch" -> "German".

languages_required:
only mandatory language requirements.

languages_preferred:
languages explicitly preferred/desirable.

languages_bonus:
languages explicitly called a plus, bonus, advantage, nice-to-have, etc.

Do not mix the three lists.
Do not put programming languages in language fields.

Examples:
  "Fluent Dutch and English required"
    required = ["Dutch", "English"]

  "English required; Dutch is a plus"
    required = ["English"]
    bonus = ["Dutch"]

  "Fluent English; German preferred"
    required = ["English"]
    preferred = ["German"]

If the advertisement has no human-language requirement, all three lists can be
empty.

8. Technical skills

technical_skills_required:
only mandatory / baseline technical requirements.

technical_skills_preferred:
preferred / plus / bonus technical skills.

9. role_summary

One concise factual sentence summarizing what the person will actually do.

10. core_responsibilities

3-8 concise core responsibilities, based only on the advertisement.

11. remote_mode

onsite / hybrid / remote only when explicit; otherwise unspecified.

12. management_responsibility

true only for actual people management/formal team leadership/hiring/performance
responsibility. Mentoring or technical leadership alone is not necessarily
people management.

13. security_clearance_required

true only when an actual security-clearance/background-security eligibility
requirement is mandatory.

14. evidence

Provide short direct evidence snippets for the decision-critical fields:
role family, mandatory experience, education, student status, languages,
seniority, management, and security clearance where applicable.

15. extraction_confidence

Overall confidence from 0.0 to 1.0.
Lower it when the advertisement is ambiguous about a decision-critical field.
""".strip()


def schema_text() -> str:

    return json.dumps(
        JobFacts.model_json_schema(),
        ensure_ascii=False,
        separators=(
            ",",
            ":",
        ),
    )


def build_job_payload(
    job: sqlite3.Row,
) -> str:

    return json.dumps(
        {
            "job_id":
                job["job_id"],

            "title":
                job["title"],

            "company":
                job["company"],

            "location":
                job["location"],

            "linkedin_job_type":
                job["job_type"],

            "linkedin_job_level":
                job["job_level"],

            "linkedin_job_function":
                job["job_function"],

            "company_industry":
                job["company_industry"],

            "description":
                job["description"],
        },
        ensure_ascii=False,
        separators=(
            ",",
            ":",
        ),
    )


def initial_prompt(
    job: sqlite3.Row,
) -> str:

    return (
        "Return JSON matching this schema:\n"
        + schema_text()
        + "\n\nJob advertisement:\n"
        + build_job_payload(
            job
        )
    )


def repair_prompt(
    job: sqlite3.Row,
    invalid_output: str,
    error_text: str,
) -> str:

    return (
        "The previous JSON failed Pydantic validation.\n"
        "Return corrected JSON only.\n\n"
        "Validation error:\n"
        + error_text
        + "\n\nInvalid output:\n"
        + invalid_output
        + "\n\nRequired schema:\n"
        + schema_text()
        + "\n\nOriginal job advertisement:\n"
        + build_job_payload(
            job
        )
    )


# =============================================================================
# ROLLING RATE HANDLER
# =============================================================================

@dataclass
class TokenReservation:
    created_at: float
    input_tokens: int


class RollingRateHandler:

    def __init__(
        self,
        rpm: int,
        input_tpm: int,
        max_concurrency: int,
    ) -> None:

        self.rpm = rpm
        self.input_tpm = input_tpm

        self.window_seconds = (
            60.0
        )

        self.request_times: deque[
            float
        ] = deque()

        self.token_reservations: deque[
            TokenReservation
        ] = deque()

        self.lock = asyncio.Lock()

        self.concurrency = (
            asyncio.Semaphore(
                max_concurrency
            )
        )

    def _prune(
        self,
        now: float,
    ) -> None:

        cutoff = (
            now
            - self.window_seconds
        )

        while (
            self.request_times
            and self.request_times[0]
            <= cutoff
        ):
            self.request_times.popleft()

        while (
            self.token_reservations
            and self.token_reservations[
                0
            ].created_at
            <= cutoff
        ):
            self.token_reservations.popleft()

    def _input_tokens_in_window(
        self,
    ) -> int:

        return sum(
            item.input_tokens
            for item
            in self.token_reservations
        )

    async def acquire(
        self,
        estimated_input_tokens: int,
    ) -> TokenReservation:

        await self.concurrency.acquire()

        try:

            while True:

                async with self.lock:

                    now = (
                        time.monotonic()
                    )

                    self._prune(
                        now
                    )

                    rpm_ok = (
                        len(
                            self.request_times
                        )
                        < self.rpm
                    )

                    used_input = (
                        self
                        ._input_tokens_in_window()
                    )

                    tpm_ok = (
                        used_input
                        + estimated_input_tokens
                        <= self.input_tpm
                    )

                    if rpm_ok and tpm_ok:

                        reservation = (
                            TokenReservation(
                                created_at=now,
                                input_tokens=(
                                    estimated_input_tokens
                                ),
                            )
                        )

                        self.request_times.append(
                            now
                        )

                        self.token_reservations.append(
                            reservation
                        )

                        return reservation

                    waits = []

                    if not rpm_ok:

                        waits.append(
                            self.window_seconds
                            - (
                                now
                                - self.request_times[0]
                            )
                        )

                    if not tpm_ok:

                        running = (
                            used_input
                        )

                        for reservation in (
                            self.token_reservations
                        ):

                            running -= (
                                reservation
                                .input_tokens
                            )

                            if (
                                running
                                + estimated_input_tokens
                                <= self.input_tpm
                            ):

                                waits.append(
                                    self.window_seconds
                                    - (
                                        now
                                        - reservation
                                        .created_at
                                    )
                                )

                                break

                    wait_for = max(
                        0.25,
                        min(
                            waits
                        )
                        if waits
                        else 1.0,
                    )

                await asyncio.sleep(
                    wait_for
                )

        except Exception:

            self.concurrency.release()

            raise

    async def reconcile(
        self,
        reservation: TokenReservation,
        actual_input_tokens: (
            int
            | None
        ),
    ) -> None:

        if (
            actual_input_tokens
            is None
            or actual_input_tokens
            <= 0
        ):
            return

        async with self.lock:

            # Mutating the reservation is safe under the lock and improves
            # subsequent scheduling in the same rolling minute.
            reservation.input_tokens = (
                actual_input_tokens
            )

    def release(
        self,
    ) -> None:

        self.concurrency.release()


def estimate_input_tokens(
    prompt: str,
) -> int:
    """
    Conservative pre-launch estimate.

    Google documents roughly 4 characters/token. We use 3.5 chars/token to
    reserve slightly more than the rough average.

    IMPORTANT: no output-token reserve is added because Gemini TPM is defined
    as input tokens per minute.
    """

    total_chars = (
        len(
            SYSTEM_INSTRUCTION
        )
        + len(
            prompt
        )
    )

    return max(
        1,
        math.ceil(
            total_chars
            / CHARS_PER_TOKEN_ESTIMATE
        ),
    )


# =============================================================================
# DATABASE
# =============================================================================

def now_local() -> str:

    return datetime.now(
        TIMEZONE
    ).isoformat()


def connect_database() -> (
    sqlite3.Connection
):
    return shared_connect(DATABASE_PATH)


def verify_database(
    connection: sqlite3.Connection,
) -> None:

    views = {
        row["name"]
        for row
        in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='view'
            """
        ).fetchall()
    }

    if "ready_for_ai" not in views:

        raise RuntimeError(
            "ready_for_ai view missing. "
            "Run setup_ai_pipeline.py first."
        )

    fact_columns = {
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(job_ai_facts)"
        ).fetchall()
    }

    required_v2_columns = {
        "role_families_json",
        "ai_data_engineering_hybrid",
        "experience_requirement_text",
        "preferred_years_experience",
        "degree_required",
        "minimum_degree_level",
        "student_status_required",
        "languages_preferred_json",
        "languages_bonus_json",
        "technical_skills_preferred_json",
        "role_summary",
        "core_responsibilities_json",
    }

    missing_v2 = (
        required_v2_columns
        - fact_columns
    )

    if missing_v2:
        raise RuntimeError(
            "AI v2 database columns are missing: "
            + ", ".join(
                sorted(
                    missing_v2
                )
            )
            + "\nRun setup_pipeline_v2.py first."
        )

    job_columns = {
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(jobs)"
        ).fetchall()
    }

    required_metadata_columns = {
        "metadata_gate_status",
        "metadata_gate_reason",
        "metadata_gate_rule_id",
        "metadata_gate_rule_key",
        "metadata_gate_evaluated_at",
        "metadata_gate_version",
    }

    missing_metadata = (
        required_metadata_columns
        - job_columns
    )

    if missing_metadata:
        raise RuntimeError(
            "Metadata-gate columns are missing: "
            + ", ".join(
                sorted(
                    missing_metadata
                )
            )
            + "\nRun setup_metadata_gate.py first."
        )


def require_api_key() -> None:

    if not SETTINGS.ai.api_key:

        raise RuntimeError(
            "GEMINI_API_KEY is missing "
            "from the environment/.env."
        )


def load_queue(
    connection: sqlite3.Connection,
    job_ids: list[str] | None = None,
) -> list[sqlite3.Row]:

    sql = """
        SELECT
            job_id,
            title,
            company,
            location,

            job_type,
            job_level,
            job_function,
            company_industry,

            description,

            human_decision,
            classifier_status,
            review_category,

            metadata_gate_status,

            ai_status,
            ai_attempt_count,

            first_seen_at

        FROM ready_for_ai

        WHERE
            COALESCE(
                ai_attempt_count,
                0
            ) < ?
    """

    params: list[object] = [
        MAX_ATTEMPTS_PER_JOB
    ]

    if job_ids is not None:
        selected = list(dict.fromkeys(job_ids))
        if not selected:
            return []
        sql += (
            "\nAND job_id IN ("
            + ",".join("?" for _ in selected)
            + ")"
        )
        params.extend(selected)

    sql += """
        ORDER BY
            CASE
                WHEN human_decision = 'KEEP'
                THEN 0
                ELSE 1
            END,

            first_seen_at,
            job_id
    """

    effective_limit = (
        1
        if AI_PROBE_MODE
        else MAX_JOBS_PER_RUN
    )

    if effective_limit > 0:

        sql += "\nLIMIT ?"

        params.append(
            effective_limit
        )

    return connection.execute(
        sql,
        tuple(
            params
        ),
    ).fetchall()


# =============================================================================
# GOOGLE GENAI
# =============================================================================

def generation_config() -> (
    types.GenerateContentConfig
):

    # Deliberately NO max_output_tokens cap.
    #
    # The schema/prompt constrain expected output. If we later observe runaway
    # outputs, we can introduce a cap based on empirical output-token data.
    return types.GenerateContentConfig(
        system_instruction=(
            SYSTEM_INSTRUCTION
        ),

        temperature=(
            TEMPERATURE
        ),

        top_p=(
            TOP_P
        ),

        top_k=(
            TOP_K
        ),

        thinking_config=(
            types.ThinkingConfig(
                thinking_level=(
                    THINKING_LEVEL
                    .lower()
                )
            )
        ),
    )


def strip_code_fence(
    text: str,
) -> str:

    value = str(
        text
        or ""
    ).strip()

    match = re.fullmatch(
        r"```(?:json)?\s*(.*?)\s*```",
        value,
        flags=(
            re.IGNORECASE
            | re.DOTALL
        ),
    )

    if match:

        return match.group(
            1
        ).strip()

    return value


def response_text(
    response,
) -> str:

    value = strip_code_fence(
        getattr(
            response,
            "text",
            "",
        )
    )

    if value:

        return value

    finish_reason = None

    candidates = getattr(
        response,
        "candidates",
        None,
    )

    if candidates:

        finish_reason = getattr(
            candidates[0],
            "finish_reason",
            None,
        )

    raise RuntimeError(
        "Google GenAI returned no final text. "
        f"finish_reason={finish_reason!r}; "
        f"thinking_level={THINKING_LEVEL}."
    )


def usage_value(
    usage,
    *names: str,
):

    if usage is None:

        return None

    for name in names:

        value = getattr(
            usage,
            name,
            None,
        )

        if value is not None:

            return value

    return None


def response_usage(
    response,
) -> dict:

    usage = getattr(
        response,
        "usage_metadata",
        None,
    )

    return {
        "input_tokens":
            usage_value(
                usage,
                "prompt_token_count",
                "input_token_count",
            ),

        "output_tokens":
            usage_value(
                usage,
                "candidates_token_count",
                "output_token_count",
            ),

        "thought_tokens":
            usage_value(
                usage,
                "thoughts_token_count",
                "thought_token_count",
            ),

        "total_tokens":
            usage_value(
                usage,
                "total_token_count",
            ),
    }


async def rate_limited_generate(
    aclient,
    handler: RollingRateHandler,
    prompt: str,
    on_acquired=None,
):

    estimated_input = (
        estimate_input_tokens(
            prompt
        )
    )

    reservation = (
        await handler.acquire(
            estimated_input
        )
    )

    try:

        if on_acquired is not None:

            await on_acquired(
                estimated_input
            )

        response = (
            await aclient.models
            .generate_content(
                model=MODEL,
                contents=prompt,
                config=(
                    generation_config()
                ),
            )
        )

        usage = response_usage(
            response
        )

        await handler.reconcile(
            reservation,
            usage[
                "input_tokens"
            ],
        )

        return response

    finally:

        handler.release()


async def extract_facts(
    aclient,
    handler: RollingRateHandler,
    job: sqlite3.Row,
    on_request_acquired=None,
):

    prompt = initial_prompt(
        job
    )

    response = (
        await rate_limited_generate(
            aclient,
            handler,
            prompt,
            on_acquired=(
                on_request_acquired
            ),
        )
    )

    raw = response_text(
        response
    )

    try:

        facts = (
            JobFacts
            .model_validate_json(
                raw
            )
        )

        return (
            response,
            facts,
        )

    except ValidationError as first_error:

        last_error = (
            first_error
        )

        for _ in range(
            MAX_SCHEMA_REPAIR_ATTEMPTS
        ):

            repair = repair_prompt(
                job,
                raw,
                str(
                    last_error
                ),
            )

            repair_response = (
                await rate_limited_generate(
                    aclient,
                    handler,
                    repair,
                    on_acquired=(
                        on_request_acquired
                    ),
                )
            )

            repair_raw = (
                response_text(
                    repair_response
                )
            )

            try:

                facts = (
                    JobFacts
                    .model_validate_json(
                        repair_raw
                    )
                )

                return (
                    repair_response,
                    facts,
                )

            except ValidationError as exc:

                raw = repair_raw
                last_error = exc

        raise RuntimeError(
            "Pydantic validation failed "
            "after repair: "
            f"{last_error}"
        )


# =============================================================================
# PERSISTENCE
# =============================================================================

def mark_attempt(
    connection: sqlite3.Connection,
    job_id: str,
) -> None:

    connection.execute(
        """
        UPDATE jobs

        SET
            ai_status='PROCESSING',

            ai_attempt_count=
                COALESCE(
                    ai_attempt_count,
                    0
                ) + 1,

            ai_last_attempt_at=?,
            ai_last_error=NULL

        WHERE job_id=?
        """,
        (
            now_local(),
            job_id,
        ),
    )

    connection.commit()


def save_success(
    connection: sqlite3.Connection,
    job_id: str,
    response,
    facts: JobFacts,
) -> None:

    timestamp = now_local()

    usage = response_usage(
        response
    )

    facts_dict = facts.model_dump()

    response_id = (
        getattr(
            response,
            "response_id",
            None,
        )
        or getattr(
            response,
            "id",
            None,
        )
    )

    role_families = []

    for family in facts.role_families:
        if family not in role_families:
            role_families.append(
                family
            )

    if (
        facts.primary_role_family
        not in role_families
    ):
        role_families.insert(
            0,
            facts.primary_role_family,
        )

    role_families = (
        role_families[:3]
    )

    ai_data_hybrid = (
        derive_ai_data_hybrid(
            role_families
        )
    )

    # These are deterministic derived fields; we do not ask the model to
    # produce redundant booleans that could contradict the atomic facts.
    phd_required = (
        facts.degree_required
        and facts.minimum_degree_level
        == "phd"
    )

    legacy_degree_requirement = (
        facts.minimum_degree_level
        if facts.degree_required
        else "none_specified"
    )

    connection.execute(
        """
        INSERT INTO job_ai_facts (
            job_id,
            schema_version,
            extractor_model,
            prompt_version,

            role_family,
            role_families_json,
            ai_data_engineering_hybrid,

            seniority,

            minimum_years_experience,
            experience_requirement_text,
            preferred_years_experience,
            preferred_experience_text,

            degree_requirement,
            degree_required,
            minimum_degree_level,
            degree_requirement_text,
            degree_fields_json,
            phd_required,
            phd_preferred,

            student_status_required,
            student_requirement_text,

            languages_required_json,
            languages_preferred_json,
            languages_bonus_json,
            language_requirement_notes,

            technical_skills_required_json,
            technical_skills_preferred_json,

            role_summary,
            core_responsibilities_json,

            remote_mode,

            management_responsibility,
            security_clearance_required,

            extraction_confidence,

            evidence_json,
            raw_extraction_json,

            response_id,
            input_tokens,
            output_tokens,
            total_tokens,

            created_at,
            updated_at
        )

        VALUES (
            ?,?,?,?,
            ?,?,?,
            ?,
            ?,?,?,?,
            ?,?,?,?,?,?,?,
            ?,?,
            ?,?,?,?,
            ?,?,
            ?,?,
            ?,
            ?,?,
            ?,
            ?,?,
            ?,?,?,?,
            ?,?
        )

        ON CONFLICT(job_id)
        DO UPDATE SET

            schema_version=
                excluded.schema_version,

            extractor_model=
                excluded.extractor_model,

            prompt_version=
                excluded.prompt_version,

            role_family=
                excluded.role_family,

            role_families_json=
                excluded.role_families_json,

            ai_data_engineering_hybrid=
                excluded.ai_data_engineering_hybrid,

            seniority=
                excluded.seniority,

            minimum_years_experience=
                excluded.minimum_years_experience,

            experience_requirement_text=
                excluded.experience_requirement_text,

            preferred_years_experience=
                excluded.preferred_years_experience,

            preferred_experience_text=
                excluded.preferred_experience_text,

            degree_requirement=
                excluded.degree_requirement,

            degree_required=
                excluded.degree_required,

            minimum_degree_level=
                excluded.minimum_degree_level,

            degree_requirement_text=
                excluded.degree_requirement_text,

            degree_fields_json=
                excluded.degree_fields_json,

            phd_required=
                excluded.phd_required,

            phd_preferred=
                excluded.phd_preferred,

            student_status_required=
                excluded.student_status_required,

            student_requirement_text=
                excluded.student_requirement_text,

            languages_required_json=
                excluded.languages_required_json,

            languages_preferred_json=
                excluded.languages_preferred_json,

            languages_bonus_json=
                excluded.languages_bonus_json,

            language_requirement_notes=
                excluded.language_requirement_notes,

            technical_skills_required_json=
                excluded.technical_skills_required_json,

            technical_skills_preferred_json=
                excluded.technical_skills_preferred_json,

            role_summary=
                excluded.role_summary,

            core_responsibilities_json=
                excluded.core_responsibilities_json,

            remote_mode=
                excluded.remote_mode,

            management_responsibility=
                excluded.management_responsibility,

            security_clearance_required=
                excluded.security_clearance_required,

            extraction_confidence=
                excluded.extraction_confidence,

            evidence_json=
                excluded.evidence_json,

            raw_extraction_json=
                excluded.raw_extraction_json,

            response_id=
                excluded.response_id,

            input_tokens=
                excluded.input_tokens,

            output_tokens=
                excluded.output_tokens,

            total_tokens=
                excluded.total_tokens,

            updated_at=
                excluded.updated_at
        """,
        (
            job_id,
            SCHEMA_VERSION,
            MODEL,
            PROMPT_VERSION,

            facts.primary_role_family,
            json.dumps(
                role_families,
                ensure_ascii=False,
            ),
            int(
                ai_data_hybrid
            ),

            facts.seniority,

            facts.minimum_years_experience,
            facts.experience_requirement_text,
            facts.preferred_years_experience,
            facts.preferred_experience_text,

            legacy_degree_requirement,
            int(
                facts.degree_required
            ),
            facts.minimum_degree_level,
            facts.degree_requirement_text,
            json.dumps(
                facts.degree_fields,
                ensure_ascii=False,
            ),
            int(
                phd_required
            ),
            int(
                facts.phd_preferred
            ),

            int(
                facts.student_status_required
            ),
            facts.student_requirement_text,

            json.dumps(
                facts.languages_required,
                ensure_ascii=False,
            ),
            json.dumps(
                facts.languages_preferred,
                ensure_ascii=False,
            ),
            json.dumps(
                facts.languages_bonus,
                ensure_ascii=False,
            ),
            facts.language_requirement_notes,

            json.dumps(
                facts.technical_skills_required,
                ensure_ascii=False,
            ),
            json.dumps(
                facts.technical_skills_preferred,
                ensure_ascii=False,
            ),

            facts.role_summary,
            json.dumps(
                facts.core_responsibilities,
                ensure_ascii=False,
            ),

            facts.remote_mode,

            int(
                facts.management_responsibility
            ),
            int(
                facts.security_clearance_required
            ),

            facts.extraction_confidence,

            json.dumps(
                [
                    item.model_dump()
                    for item in facts.evidence
                ],
                ensure_ascii=False,
            ),

            json.dumps(
                facts_dict,
                ensure_ascii=False,
            ),

            response_id,

            usage[
                "input_tokens"
            ],
            usage[
                "output_tokens"
            ],
            usage[
                "total_tokens"
            ],

            timestamp,
            timestamp,
        ),
    )

    connection.execute(
        """
        UPDATE jobs

        SET
            ai_status='EXTRACTED',
            ai_extracted_at=?,
            ai_last_error=NULL

        WHERE job_id=?
        """,
        (
            timestamp,
            job_id,
        ),
    )

    connection.commit()


def save_failure(
    connection: sqlite3.Connection,
    job_id: str,
    reason: str,
) -> None:

    connection.execute(
        """
        UPDATE jobs

        SET
            ai_status='FAILED',
            ai_last_error=?

        WHERE job_id=?
        """,
        (
            reason[:4000],
            job_id,
        ),
    )

    connection.commit()


# =============================================================================
# WORKER
# =============================================================================

async def process_job(
    aclient,
    handler: RollingRateHandler,
    db_lock: asyncio.Lock,
    job: sqlite3.Row,
    index: int,
    total: int,
    run_post_ai_after_extraction: bool = True,
) -> tuple[
    str,
    str,
]:

    print(
        f"[{index}/{total}] QUEUED "
        f"{job['title']} — "
        f"{job['company']}"
    )

    attempts_remaining = max(
        0,
        MAX_ATTEMPTS_PER_JOB
        - int(job["ai_attempt_count"] or 0),
    )
    attempts_this_run = 0
    retry_number = 0

    async def on_request_acquired(
        estimated_input_tokens: int,
    ) -> None:

        nonlocal attempts_remaining
        nonlocal attempts_this_run

        if attempts_remaining <= 0:
            raise RuntimeError(
                "AI provider attempt limit reached "
                f"for {job['job_id']}"
            )

        async with db_lock:

            connection = (
                connect_database()
            )

            try:

                mark_attempt(
                    connection,
                    job["job_id"],
                )

            finally:

                connection.close()

        attempts_remaining -= 1
        attempts_this_run += 1

        print(
            f"[{index}/{total}] SEND "
            f"{job['job_id']} | "
            f"attempt={attempts_this_run} | "
            f"estimated_input≈"
            f"{estimated_input_tokens}"
        )

    while attempts_remaining > 0:

        try:

            response, facts = (
                await extract_facts(
                    aclient,
                    handler,
                    job,
                    on_request_acquired=(
                        on_request_acquired
                    ),
                )
            )

        except Exception as exc:

            reason = (
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            async with db_lock:

                connection = (
                    connect_database()
                )

                try:

                    save_failure(
                        connection,
                        job["job_id"],
                        reason,
                    )

                finally:

                    connection.close()

            if (
                not is_transient_ai_error(exc)
                or attempts_remaining <= 0
            ):
                print(
                    f"[{index}/{total}] FAILED "
                    f"{job['job_id']}: "
                    f"{reason}"
                )

                return (
                    "FAILED",
                    job["job_id"],
                )

            retry_number += 1
            delay = retry_delay_seconds(
                retry_number,
                base_seconds=AI_RETRY_BASE_SECONDS,
                max_seconds=AI_RETRY_MAX_SECONDS,
            )
            print(
                f"[{index}/{total}] RETRY "
                f"{job['job_id']} in {delay:.1f}s | "
                f"{reason}"
            )
            await asyncio.sleep(delay)
            continue

        break

    else:
        reason = (
            "AI provider attempt limit reached "
            f"for {job['job_id']}"
        )
        async with db_lock:
            connection = connect_database()
            try:
                save_failure(connection, job["job_id"], reason)
            finally:
                connection.close()
        print(
            f"[{index}/{total}] FAILED "
            f"{job['job_id']}: {reason}"
        )
        return (
            "FAILED",
            job["job_id"],
        )

    post_ai_result = None
    post_ai_error = None

    async with db_lock:

        connection = (
            connect_database()
        )

        try:

            save_success(
                connection,
                job["job_id"],
                response,
                facts,
            )

            if run_post_ai_after_extraction:
                try:
                    post_ai_result = (
                        evaluate_and_persist_post_ai(
                            connection,
                            job["job_id"],
                        )
                    )
                    connection.commit()

                except Exception as exc:
                    post_ai_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

        finally:

            connection.close()

    usage = response_usage(
        response
    )

    post_label = (
        post_ai_result.post_ai_status
        if post_ai_result is not None
        else "POST_AI_PENDING"
    )

    print(
        f"[{index}/{total}] DONE "
        f"{job['job_id']} | "
        f"{facts.primary_role_family} | "
        f"{facts.seniority} | "
        f"min_years="
        f"{facts.minimum_years_experience} | "
        f"student={facts.student_status_required} | "
        f"required_langs={facts.languages_required} | "
        f"post_ai={post_label} | "
        f"input={usage['input_tokens']} | "
        f"output={usage['output_tokens']} | "
        f"thoughts={usage['thought_tokens']}"
    )

    if post_ai_error:
        print(
            f"[{index}/{total}] POST_AI WARNING "
            f"{job['job_id']}: {post_ai_error}"
        )

    return (
        "EXTRACTED",
        job["job_id"],
    )


# =============================================================================
# MAIN
# =============================================================================

async def async_main(
    job_ids: list[str] | None = None,
    run_post_ai_after_extraction: bool = True,
):

    require_api_key()

    connection = (
        connect_database()
    )

    try:

        verify_database(
            connection
        )

        queue = load_queue(
            connection,
            job_ids=job_ids,
        )

    finally:

        connection.close()

    print()
    print("=" * 96)
    print(
        "GOOGLE GENAI / GEMMA JOB EXTRACTION"
    )
    print("=" * 96)

    print(
        f"SDK:               google-genai"
    )

    print(
        f"Model:             {MODEL}"
    )

    print(
        f"Thinking:          "
        f"{THINKING_LEVEL}"
    )

    print(
        f"Sampling:          "
        f"T={TEMPERATURE}, "
        f"top_p={TOP_P}, "
        f"top_k={TOP_K}"
    )

    print(
        f"Queued jobs:       "
        f"{len(queue)}"
    )

    print(
        f"Target RPM:        "
        f"{TARGET_RPM}"
    )

    print(
        f"Max concurrency:   "
        f"{MAX_CONCURRENCY}"
    )

    print(
        f"Input TPM limit:   "
        f"{PROVIDER_TPM}"
    )

    print(
        f"Effective input TPM: "
        f"{EFFECTIVE_TPM}"
    )

    print(
        "Output cap:        none "
        "(model default)"
    )

    if not queue:

        print(
            "Nothing is ready for AI."
        )

        return {
            "processed": 0,
            "extracted": 0,
            "failed": 0,
            "job_ids": [],
        }

    handler = (
        RollingRateHandler(
            rpm=TARGET_RPM,
            input_tpm=EFFECTIVE_TPM,
            max_concurrency=(
                MAX_CONCURRENCY
            ),
        )
    )

    db_lock = asyncio.Lock()

    # Official Google GenAI asynchronous client.
    async with genai.Client(
        api_key=SETTINGS.ai.api_key
    ).aio as aclient:

        tasks = [
            asyncio.create_task(
                process_job(
                    aclient,
                    handler,
                    db_lock,
                    job,
                    index,
                    len(queue),
                    run_post_ai_after_extraction,
                )
            )
            for index, job
            in enumerate(
                queue,
                start=1,
            )
        ]

        results = (
            await asyncio.gather(
                *tasks
            )
        )

    extracted = sum(
        1
        for status, _
        in results
        if status == "EXTRACTED"
    )

    failed = sum(
        1
        for status, _
        in results
        if status == "FAILED"
    )

    print()
    print("=" * 96)
    print("AI EXTRACTION SUMMARY")
    print("=" * 96)

    print(
        f"Extracted:          "
        f"{extracted}"
    )

    print(
        f"Failed:             "
        f"{failed}"
    )

    return {
        "processed": len(results),
        "extracted": extracted,
        "failed": failed,
        "job_ids": [job_id for _, job_id in results],
    }


def main():

    asyncio.run(
        async_main(
            run_post_ai_after_extraction=True,
        )
    )


if __name__ == "__main__":
    main()
