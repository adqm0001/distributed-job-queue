# distributed-job-queue

A distributed job queue in Go, built on Redis. Reliable at-least-once delivery, crash recovery, task deduplication, idempotent handlers, and scheduled jobs.

Built this to actually understand how job queues work under the hood, the stuff Sidekiq and asynq do for you: at-least-once delivery, what happens when a worker dies mid-job, how you stop the same task from running twice, how "run this in an hour" really works. It's all implemented from scratch on Redis, not a wrapper around an existing queue.

## What it does

- **At-least-once delivery** - workers reserve a job, then ack it when done. If a worker dies mid-job the job isn't lost, it gets picked back up.
- **Crash recovery** - a background reaper requeues jobs whose lease expired, meaning the worker holding them died.
- **Dead-letter queue** - jobs that fail too many times get parked instead of retrying forever.
- **Task deduplication** - opt-in unique keys so the same task can't be queued twice while it's already in flight.
- **Idempotent handlers** - middleware that makes at-least-once safe for stuff like charging a card, where running it twice is a real problem.
- **Scheduled jobs** - "run this in an hour." The run-at time is scored off the Redis server clock, so producers on different machines agree on when a job is due.
- **Distributed** - producers and workers are separate processes. Run `--scale worker=3` and the jobs load-balance across them on their own.

## How it works

The reliable broker keeps a few structures in Redis and moves jobs between them with Lua scripts, so every state change is atomic:

```
  producer                          worker
     │  Submit                         │  Dequeue (reserve)
     ▼                                 ▼
 ┌─────────┐   reserve (Lua)     ┌──────────┐   ack (Lua)
 │ pending │ ──────────────────▶ │  active  │ ───────────▶  done (job deleted)
 │  LIST   │                     │  ZSET    │   nack (Lua)
 └─────────┘ ◀────────────────── └──────────┘ ───────────▶  retry (back to pending)
     ▲          reaper requeues        │                    or dead (after max attempts)
     │          expired leases         ▼
 ┌───────────┐                    ┌────────┐
 │ scheduled │ ── PromoteDue ───▶ │  dead  │
 │   ZSET    │    (Lua, atomic)   │  LIST  │
 └───────────┘                    └────────┘

  task:<id>  HASH  →  { data, state, attempts, unique? }
```

- **pending** - list of job IDs waiting to run
- **active** - sorted set of in-flight IDs, scored by when they were reserved. The reaper watches this for expired leases.
- **scheduled** - sorted set of delayed IDs, scored by run-at time. `PromoteDue` moves the due ones into pending.
- **dead** - IDs that ran out of attempts
- **task:&lt;id&gt;** - a hash per job holding its payload, state, and attempt count

Every multi-step move (reserve, ack, nack, reap, promote) is a single Lua script, so a crash at the wrong moment can't lose or duplicate a job.

## Idempotency is your job

This delivers at-least-once, so a job can run more than once (never zero). The queue does not make that safe for you, and no queue really can, Sidekiq and asynq included. If running a job twice would be bad, like charging a card, your handler has to handle that itself. There's middleware in here to help, but the broker doesn't solve it for you.

## Running it

Needs Go and Docker.

```bash
# in-memory demo, no setup needed (this is where the scheduling policies run)
go run ./cmd/demo

# distributed version (Redis + 3 workers)
docker compose up --build --scale worker=3
go run ./cmd/producer          # in another terminal
```

## Layout

```
cmd/
  demo/       in-memory single-process demo, no Redis needed
  producer/   submits jobs
  worker/     worker pool + reaper + scheduler
internal/
  job/         Job struct, status + priority
  policy/      scheduling policies (FIFO, priority, weighted-fair) for the in-memory queue
  queue/       in-memory queue (mutex + cond)
  worker/      worker pool, handlers, ack/nack loop
  broker/      the Redis brokers, including the reliable one
  idempotency/ the idempotent-handler middleware
```
