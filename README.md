# Job Simple Search

LinkedIn discovery and review pipeline backed by SQLite. Rules remain editable
in the database and the web UI, while the UI, scheduler, and CLI share the same
service functions.

## Pipeline

`DISCOVERY -> PRE_DESCRIPTION -> human review -> DETAILS -> METADATA_GATE -> Gemma extraction -> POST_AI`

A LinkedIn `job_id` is PRE_DESCRIPTION-classified only when first discovered.
Rediscovery updates `last_seen_at` and records a search-hit observation without
resetting human, detail, AI, or final-decision state. Historical rule application
is available only through explicit reapply actions in Rules / Settings.

The web UI places **Pre-Fetch Results** between Review Inbox and Fetched Jobs.
Its Automatic rejections, Human-reviewed, and All results sub-tabs show only
jobs whose descriptions have not yet been fetched. Each view uses the same
company/title grouping, run scope, group pagination, selection, KEEP, and
EXCLUDE interaction model as Review Inbox. A manual decision overrides the PRE
classifier while preserving that original classifier result for auditing.

A batch becomes `COMPLETE` only when every batch member with fetched details and
`metadata_gate_status=PASS` has a terminal `SHORTLIST`, `REVIEW`, or `REJECT`
POST_AI result. Transient provider failures are retried up to
`AI_MAX_ATTEMPTS_PER_JOB` with bounded exponential backoff and jitter. Configure
the delay bounds with `AI_RETRY_BASE_SECONDS` and `AI_RETRY_MAX_SECONDS`. If any
eligible job remains unfinished, the batch stays `FAILED`; running `continue`
again resumes unfinished work without reprocessing terminal jobs.

## Local setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

For this existing checkout, the default path remains `./jobs.db`. Set
`JOBSEARCH_DB_PATH=./data/jobs.db` only after moving/copying the database
deliberately. Docker Compose overrides the path to `/app/data/jobs.db`.
Migrations are additive:

```powershell
simplejobsearch migrate
```

## Commands

```powershell
# NiceGUI at http://localhost:5000 (WEB_HOST/WEB_PORT are configurable)
simplejobsearch web

# APScheduler foreground service (00:00 search, 08:00 reminder by default)
simplejobsearch scheduler

# Manual discovery/search
simplejobsearch search

# Continue the latest reviewed batch
simplejobsearch continue

# Run the exact functions registered with APScheduler, on demand
simplejobsearch run-nightly
simplejobsearch run-review-reminder

# Test each production email template without changing workflow state
simplejobsearch email-test search
simplejobsearch email-test review
simplejobsearch email-test complete
```

Equivalent module commands use `python -m simplejobsearch.cli <command>`.

## Public/mobile access

`WEB_HOST` and `WEB_PORT` control where NiceGUI listens locally.
`PUBLIC_BASE_URL` is the separate HTTPS address placed in notification emails:

```dotenv
WEB_HOST=0.0.0.0
WEB_PORT=5000
PUBLIC_BASE_URL=https://your-domain.ngrok-free.app
```

Trailing slashes are removed during configuration loading. Never derive public
links from `WEB_HOST`; `0.0.0.0` is a bind address, not a user-facing URL. The
shared NiceGUI root supports `/review` for Review Inbox and `/results` for
Post-AI Extraction without duplicating the UI.

Search and review messages link to `/review`; pipeline-completion messages link
to `/results`. Each message contains both a plain-text URL and an HTML action
button. If `PUBLIC_BASE_URL` is empty, messages are still sent without a link
and a warning is logged.

For manual ngrok testing, start `simplejobsearch web`, then expose port 5000
with an OAuth-protected ngrok endpoint. Keep the ngrok authtoken in ngrok's own
agent configuration, never in this repository. Set `PUBLIC_BASE_URL` only after
the HTTPS endpoint and authentication flow work from a phone over mobile data.

## Windows Task Scheduler lifecycle

Windows automation is opt-in. It uses Windows Task Scheduler instead of the
long-running `simplejobsearch scheduler` command and registers four tasks:

- `JobSimpleSearch-Nightly` at 22:30 searches first, starts NiceGUI/ngrok, and
  then sends the search email.
- `JobSimpleSearch-ReviewReminder` at 08:00 ensures the portal is running before
  sending the reminder.
- `JobSimpleSearch-Continue` is an on-demand independent worker started by the
  Continue Pipeline button.
- `JobSimpleSearch-ClosePortal` is an on-demand independent shutdown worker
  used by the Continue Pipeline and Close review portal buttons.

The nightly and reminder workers check both the local NiceGUI endpoint and the
configured ngrok tunnel before sending an email. A healthy existing portal is
reused without running the startup script. If either service is unavailable,
the worker restores the portal before sending an email containing its link.

Enable the handoff in `.env`:

```dotenv
WINDOWS_TASK_AUTOMATION=true
WINDOWS_PIPELINE_TASK_NAME=JobSimpleSearch-Continue
WINDOWS_PORTAL_STOP_TASK_NAME=JobSimpleSearch-ClosePortal
WINDOWS_PORTAL_SHUTDOWN_DELAY_SECONDS=3
```

Stop manually launched NiceGUI, ngrok, and APScheduler processes before the
first automated run, then register the tasks from PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\Register-Tasks.ps1
```

The registered tasks use the current interactive Windows account, so that user
must remain logged on. The tasks are configured with `WakeToRun` and
`StartWhenAvailable`, but Windows power policy must still permit wake timers.

With automation enabled, Continue Pipeline starts the independent scheduled
worker and then closes the managed NiceGUI/ngrok processes. The worker performs
details, metadata, AI, and Post-AI processing. On success it restarts the portal
before sending the results email. If the pipeline fails, it also attempts to
restore the portal so retry controls remain reachable. The Post-AI page ends
with a **Close review portal** button that shuts down only the PID-validated
NiceGUI and ngrok processes started by these scripts.

Runtime PID files and process logs are written under `data/windows-runtime/`.
The scripts refuse to kill an unrelated process and refuse to take over an
already-running unmanaged server or tunnel.

For a manual lifecycle check:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\Start-ReviewPortal.ps1
Start-ScheduledTask -TaskName JobSimpleSearch-Continue
powershell -ExecutionPolicy Bypass -File .\scripts\windows\Stop-ReviewPortal.ps1
```

Keep authentication secrets out of `ngrok-policy.yml`. ngrok supports resolving
Traffic Policy credentials from its secrets vault; use that before relying on
unattended startup.

## Email

Set `EMAIL_ENABLED=true` plus `SMTP_HOST`, `SMTP_PORT`, `EMAIL_FROM`, and
`EMAIL_TO`. `SMTP_USERNAME`/`SMTP_PASSWORD` are optional for relays that do not
authenticate. With email disabled, notification calls log and return without
failing the workflow. Scheduler and pipeline results include a separate
`notification` object with `SENT`, `SKIPPED`, `FAILED`, or `NOT_ATTEMPTED`
status. An SMTP failure is reported there and does not change a successfully
completed search or pipeline batch to `FAILED`.

The `email-test` commands use synthetic summaries and call the same production
email functions as the workflow. They return exit code 0 only for `SENT`; a
disabled or failed delivery returns a nonzero exit code. Gmail accounts normally
require an app password rather than the account password.

SMTP transport security is explicit and is never inferred from the port.
Supported `SMTP_SECURITY` values are `ssl`, `starttls`, and `none`; the default
is `starttls`. Gmail implicit TLS uses:

```dotenv
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_SECURITY=ssl
```

A provider using explicit STARTTLS normally uses:

```dotenv
SMTP_PORT=587
SMTP_SECURITY=starttls
```

When `SMTP_SECURITY` is absent, the legacy `SMTP_USE_TLS` setting remains a
compatibility fallback.

## Docker preparation

The Compose file uses one image for `web` and `scheduler`, with both services
mounting `./data:/app/data`. Put a deliberate copy of the database at
`data/jobs.db` before starting containers:

```powershell
docker compose build
docker compose up
```

SQLite remains in WAL mode. Run only one scheduler replica and keep the shared
volume on a filesystem with reliable file locking.

## Query metrics

The production baseline has five logical categories in two markets: five
Netherlands searches and five Switzerland searches. The Netherlands keys retain
their original names; Switzerland uses corresponding `*_ch` keys so country
performance remains independently measurable. Each query's database-backed
`country` and `location` are authoritative; `SEARCH_COUNTRY` and
`SEARCH_LOCATION` are fallbacks for rows without explicit values.

Each search run records hits, unique and new jobs, PRE_DESCRIPTION outcomes,
metadata outcomes, and final outcomes in `search_query_metrics`, allowing query
and market changes to be measured rather than guessed. Adding Switzerland
doubles the configured searches from five to ten and therefore also increases
the discovery runtime and request volume.
