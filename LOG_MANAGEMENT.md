# Log Management

## 1. Purpose

This document defines the logging standard applied across services in this
platform: what gets logged, in what format, at what level, where it is
stored, and for how long. It uses generic, technology-neutral terminology so
it can be applied to any service, and cites this repository's accounting
microservice as the reference implementation of each rule.

## 2. Principles

- **Structured, not free-text.** Every log line carries the same fixed set of
  fields so it can be parsed, filtered, and correlated mechanically rather
  than read one line at a time.
- **Correlatable across boundaries.** A single unit of work (a request, a
  queued message, an external callback) must be traceable through every
  component it touches via one shared identifier.
- **Cheap on the hot path.** Logging must not become a throughput bottleneck
  or a source of unbounded memory growth under load.
- **No secrets, no sensitive data.** Log content is treated as something that
  may be read by a broad set of engineers and shipped to third-party tooling;
  it must never leak credentials or sensitive personal data.
- **Consistent regardless of destination.** Console output and on-disk files
  carry identical structure so tooling doesn't need two parsers.

## 3. Log Format

Each log line is a single-line, structured record with a fixed field order:

```
[timestamp] level [logger][thread][correlation-id][actor][entity-id] message
```

Reference implementation (`quarkus.log.console.format` /
`quarkus.log.file.format` in `application.yml`):

```
[%d{yyyy-MM-dd'T'HH:mm:ss.SSSZ}] %-5p [%c][%t][%X{traceId}][%X{userName}][%X{sessionId}] %s%e%n
```

| Field | Generic meaning | Reference field |
|---|---|---|
| timestamp | ISO-8601 timestamp with timezone offset | `%d{yyyy-MM-dd'T'HH:mm:ss.SSSZ}` |
| level | Severity: `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` | `%-5p` |
| logger | Fully-qualified name of the emitting component | `%c` |
| thread | Executing thread, useful for diagnosing concurrency issues | `%t` |
| correlation-id | Identifier shared by every log line belonging to one unit of work | `%X{traceId}` |
| actor | The user/subscriber/account the operation is being performed for | `%X{userName}` |
| entity-id | The business or session identifier the operation is acting on | `%X{sessionId}` |
| message | The human-readable message, plus a rendered exception/stack trace if present | `%s%e` |

Contextual fields (correlation id, actor, entity id) are propagated via a
thread-local logging context (e.g. SLF4J MDC) rather than embedded manually
in each call, so any log statement emitted while handling a unit of work
automatically carries them.

## 4. Correlation IDs

Every unit of work is assigned a correlation identifier at the point it
enters the service:

- If the caller supplies one (e.g. on an inbound request or event), it is
  reused so traceability survives across service boundaries.
- If none is supplied, the service generates one at the entry point (API
  handler, message consumer, scheduled job) before any other processing
  occurs.
- The identifier is set in the logging context immediately and cleared (or
  restored to a prior value) once the unit of work completes, including on
  the failure path.
- ID generation must be allocation-light and safe under high concurrency
  (reference: `TraceIdGenerator`, which combines epoch millis with random hex
  using a thread-local RNG instead of formatting a timestamp object per
  call).

This is what makes it possible to pull every log line related to one
request/transaction/event across multiple components and threads with a
single filter.

## 5. Log Levels

| Level | Use for |
|---|---|
| ERROR | The operation failed and requires attention; always includes the causing exception when one exists. |
| WARN | An abnormal but recoverable condition (fallback path taken, retry, degraded mode). |
| INFO | Significant lifecycle events: request/response boundaries, state transitions, startup/shutdown. |
| DEBUG | Detailed internal state useful for diagnosing an issue; guarded by a level check so it's free when disabled. |
| TRACE | Fine-grained step-by-step detail, off by default; also guarded by a level check. |

Guidance:

- Default level is `INFO` in normal operation.
- DEBUG/TRACE statements must check `isDebugEnabled()`/`isTraceEnabled()`
  before building the message, so message construction cost is only paid
  when the level is actually active.
- ERROR statements must log the throwable object itself (not just
  `exception.getMessage()`), so the stack trace is preserved.
- Failure/fallback paths get a single, purpose-built log call rather than
  scattering ad hoc error logging across call sites (reference:
  `FailoverPathLogger`, a small dedicated helper for "fallback path
  activated" events).

## 6. Centralized Logging Helper

Log statements are not written ad hoc against the raw logger API. A shared
utility wraps the logger and:

- Enforces the field order and format described in Section 3.
- Reads correlation/actor/entity fields from the logging context rather than
  requiring every call site to pass them explicitly.
- Performs level checks before building the formatted message, to avoid
  paying string-formatting cost when the level is disabled.
- Uses a pooled, reusable buffer per thread for message construction instead
  of allocating a new buffer per call, since this runs on a high-throughput
  path.

Reference implementation: `LoggingUtil` (present in both modules of this
repository), exposing `logInfo` / `logDebug` / `logWarn` / `logError` /
`logTrace`, each taking the logger, the calling method name, a message
template, and template arguments.

## 7. Sensitive Data

- Credentials, shared secrets, tokens, and full request/response payloads
  that may contain personal data are never written to logs.
- Only identifiers needed for correlation and troubleshooting (correlation
  id, actor, entity id) are logged — not the underlying secret material used
  to authenticate that actor.
- Configuration values that are secrets (passwords, shared keys) must not be
  echoed into log output even at DEBUG/TRACE level.

## 8. Output Destinations

Two destinations are enabled simultaneously, using the same structured
format:

1. **Console** — for local development and for collection by a container/
   orchestration platform's log driver.
2. **File** — a rotating file on disk, for environments where a persistent,
   independently queryable copy is required regardless of container
   lifecycle.

Both destinations can be configured with independent minimum levels. For
example, a service may keep console output at `INFO` for general visibility
while restricting the file sink to `ERROR` to reduce disk volume — the two
sinks do not have to match.

## 9. Asynchronous Logging

Log writes are dispatched through a bounded, asynchronous queue per sink
rather than blocking the calling thread on I/O:

- Each sink (console, file) has its own queue with a configured maximum
  length.
- When the queue is full, the overflow policy is **discard** (drop the
  newest log events) rather than **block** (stall the producing thread).
  This trades a small amount of log completeness under extreme burst load
  for guaranteed application throughput — logging must never become a
  backpressure source for the business logic it's observing.
- Queue depth is sized relative to expected peak throughput and the number
  of log statements emitted per unit of work, not left at a generic default.

## 10. File Rotation and Retention

On-disk log files are rotated by:

- **Size** — a new file is started once the current one reaches a configured
  maximum size.
- **Count** — only a bounded number of historical (rotated) files are
  retained; the oldest is deleted once the limit is exceeded.
- **Date** — rotated files carry a date suffix so multiple days of history
  remain distinguishable at a glance.
- **Restart** — a rotation is also forced on service startup, so each
  process lifetime starts with a fresh active file.

This bounds disk usage deterministically regardless of traffic volume,
independent of any external log-shipping process.

## 11. Storage Location

- Log files are written to a well-known, fixed path (not colocated with
  application binaries), and that path is distinct per service/host so
  multiple instances never write to the same file.
- In containerized/orchestrated deployments, that path is backed by storage
  that outlives the container — a mounted volume rather than the container's
  writable layer — so logs survive pod restarts and can be inspected after a
  crash.

## 12. Per-Component Level Overrides

The global log level can be overridden per logger category (typically a
package or a specific library), independent of the service's own log level.
This is used to:

- Quiet noisy third-party dependencies (a messaging client, a configuration
  loader) that log more verbosely than needed at the default level.
- Raise or lower verbosity for the service's own code paths without
  affecting dependency logging.

## 13. Environment-Specific Tuning

Logging configuration (levels, async queue depth, rotation size) is treated
as an environment-specific concern, not a constant:

- Higher expected throughput environments size async queues larger to absorb
  burst traffic without discarding under normal peak load.
- Lower environments may run at a more verbose default level (e.g. DEBUG)
  for troubleshooting, while production defaults to `INFO`/`ERROR` to limit
  volume and cost.

## 14. Relationship to Metrics and Tracing

Logs are one of three complementary observability signals:

- **Logs** — discrete, human-readable events with full context, used for
  root-cause investigation of a specific unit of work.
- **Metrics** — aggregated numeric time series (throughput, latency,
  saturation), used for trend detection and alerting.
- **Traces** — timing/causality across service boundaries for a single unit
  of work.

Logs should not be used as a substitute for metrics (e.g. do not log a line
per event purely to count volume elsewhere — export a counter instead), and
correlation IDs used in logs should be reusable as the join key if
distributed tracing is introduced later.

## 15. Anti-Patterns

- Logging directly through `System.out`/`System.err` instead of the shared
  logging facility.
- Building a formatted log message before checking whether the target level
  is enabled.
- Concatenating strings to build a log message instead of using a
  parameterized template.
- Logging full request/response bodies, credentials, or other sensitive
  fields "just in case."
- Letting log I/O block the request/processing thread (unbounded or
  synchronous appenders on a high-throughput path).
- Emitting a log statement without a correlation id when one is available in
  context.
- Catching an exception and logging only its message, discarding the
  throwable (and therefore the stack trace).
