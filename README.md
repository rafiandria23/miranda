# Miranda

A Rust-based durable workflow engine. Run task graphs locally with zero
infrastructure, or scale out to a distributed fleet of workers coordinated by
a real control plane, backed by Postgres, MySQL, or SQLite.

Miranda borrows ideas from Temporal (workflow/activity model, retry policy,
worker polling) and Kafka (pull-based consumption, broker-agnostic queueing).
It is deliberately not a from-scratch Kubernetes. It aims to be small,
honest about what it does and doesn't do yet, and built in layers that each
work completely before the next one is added.

## What you get today

- **Run workflows with zero setup.** `miranda run workflow.yaml` executes a
  task DAG in a single process. No server, no network, no database beyond a
  local SQLite file.
- **Scale out when you need to.** `miranda-server` coordinates any number of
  worker processes over gRPC, backed by a real, durable Postgres, MySQL, or
  SQLite database. Proven under concurrent multi-worker load, not just
  single-worker happy paths.
- **Four built-in task types**: `shell`, `http`, `wait`, `noop`. Enough to
  express real automation without needing custom code yet (a plugin/SDK
  system for arbitrary user code is planned, see Roadmap below).
- **A real HTTP API and CLI** for registering workflows, submitting
  executions, and checking status against a running server.

## Quick start: run a workflow locally

```bash
cargo build -p miranda-cli
```

```yaml
# workflow.yaml
name: hello-miranda
timeout: 60

tasks:
  say_hello:
    type: shell
    command: "echo 'hello from Miranda'"

  check:
    type: http
    method: GET
    url: https://postman-echo.com/status/200
    depends_on: [say_hello]

  pause:
    type: wait
    duration: 2
    depends_on: [check]

  finish:
    type: noop
    message: "workflow complete"
    depends_on: [pause]
```

```bash
cargo run -p miranda-cli -- run workflow.yaml
```

That's it. No server, no database setup. Miranda stores execution state in
`~/.miranda/miranda.sqlite` (SQLite) by default.

## Running distributed

Miranda's distributed mode has three roles a process can take on, via two
independent flags. Starting a control plane generates (or accepts) a join
token and prints the exact command to connect a worker to it:

```bash
# Control plane only. Coordinates workers, exposes gRPC + HTTP APIs.
# --advertise-address is the address other control-plane instances should
# dial to reach this one (used for HA, see below) -- required whenever
# --grpc-bind is an unspecified address like 0.0.0.0, since that's valid
# to bind to but never valid for a peer to actually dial.
# Prints: join a worker with: miranda-server --worker --control-plane-url ... --join-token ...
miranda-server --control-plane --database-url postgres://user:pass@localhost/miranda \
  --advertise-address 127.0.0.1

# Worker only. Connects to a control plane elsewhere. --join-token is required
# and is checked by the control plane before the worker is allowed to register.
miranda-server --worker --control-plane-url http://localhost:7433 \
  --join-token <token-from-the-control-plane's-startup-output> \
  --capabilities shell,http,wait,noop

# Both, co-located in one process. Worker talks to the control plane
# in-process, no network hop, no token needed for that internal link.
# The natural shape for a single-node setup.
miranda-server --control-plane --worker --database-url sqlite:///tmp/miranda.sqlite \
  --advertise-address 127.0.0.1
```

The join token is persisted in the same database as everything else, so it
survives a control-plane restart without needing to be regenerated or
re-distributed to already-connected workers.

Then, from anywhere that can reach the control plane's HTTP API:

```bash
cargo run -p miranda-cli -- submit workflow.yaml --server http://localhost:8080
cargo run -p miranda-cli -- status <execution-id> --server http://localhost:8080
```

## High availability

Run more than one `--control-plane` instance against the same
`--database-url` and they'll automatically coordinate, no separate cluster
configuration needed, the shared database is the cluster:

```bash
# Instance 1
miranda-server --control-plane --database-url postgres://user:pass@localhost/miranda \
  --grpc-bind 0.0.0.0:7433 --http-bind 0.0.0.0:8080 --advertise-address 127.0.0.1

# Instance 2, same database, different ports
miranda-server --control-plane --database-url postgres://user:pass@localhost/miranda \
  --grpc-bind 0.0.0.0:7434 --http-bind 0.0.0.0:8081 --advertise-address 127.0.0.1
```

`--advertise-address` defaults to the `--grpc-bind` host, but Miranda refuses to
start if that resolves to an unspecified address like `0.0.0.0` -- pass it
explicitly (`127.0.0.1` for same-machine testing, or this instance's real,
reachable IP/hostname for a genuine multi-machine deployment). This is what
lets instances find and dial each other; without a real address here, peer
discovery has nothing valid to connect to.

Both instances stay fully active for worker traffic (task claiming, result
reporting, workflow submission, all already safe under concurrent access via
the shared database's own locking). One instance is elected leader via a
lease held in the database and automatically hands off if that instance goes
down. Task-ready notifications also propagate directly between instances, so
a worker connected to any instance gets woken promptly regardless of which
instance actually enqueued the work.

All three SQL backends (Postgres, MySQL, SQLite) are fully supported for
distributed mode, including the task queue and worker registry, which are
durable and shared across the whole database, not held in one process's
memory. A control plane restart doesn't lose in-flight work.

## Architecture, in one paragraph

A workflow is a DAG of tasks (`WorkflowDefinition` to `WorkflowTask`, with
dependencies). An execution is one run of a workflow. In embedded mode,
`EmbeddedEngine` drives an execution directly against a `TaskExecutor`. No
network involved. In distributed mode, a `ControlPlane` coordinates any
number of workers via leases, a durable task queue, and capability-based
routing; `miranda-server` exposes this over gRPC (worker-facing) and HTTP
(client-facing). Both modes share the same domain model (`miranda-core`) and
the same task executors (`miranda-worker`). Nothing about how a `shell` or
`http` task runs changes based on which mode you're in.

For the full crate-by-crate architecture, conventions, and design decisions,
see `MIRANDA_ARCHITECTURE.md`. For current status, what's verified versus
what's merely compiled, and the open backlog, see `MIRANDA_ROADMAP.md`.

## Task types

| Type | Purpose | Key fields |
| --- | --- | --- |
| `shell` | Run a command | `command`, `env`, `cwd`, `shell`, `success_codes` |
| `http` | Make an HTTP request | `method`, `url`, `query`, `headers`, `body`, `success_codes` |
| `wait` | Pause for a duration or until a timestamp | `duration` or `until` |
| `noop` | No-op marker, resolved by the orchestrator without touching a worker | `message` (optional) |

Every task supports `depends_on: [other_task_names]` and an optional
per-task `timeout`, overriding the workflow-level default.

## Project layout

```
crates/
├── core             # domain model, no I/O
├── worker           # task execution
├── storage          # storage-primitive traits + in-memory impl
├── storage-postgres # Postgres implementations
├── storage-mysql    # MySQL implementations
├── storage-sqlite   # SQLite implementations
├── engine           # embedded-mode orchestration
├── control-plane    # distributed-mode coordination
├── server           # the miranda-server binary: gRPC + HTTP
└── cli              # the miranda-cli binary: run/submit/status/db
```

See [`crates/README.md`](crates/README.md) for what's actually inside each
one — file-by-file structure, where a given change belongs, and the
conventions worth knowing before you dig in. See
[`proto/README.md`](proto/README.md) for the two gRPC services'
`.proto` layout.

## Status

Miranda is under active development. Embedded execution and distributed
execution (including durable, SQL-backed coordination across all three
supported databases) are built and verified under real concurrent load.
Distributed mode includes workers waking immediately on task availability
instead of waiting on a poll timer, a Kubernetes-kubeadm-style `init`/`join`
flow with join-token authentication, and high availability, multiple
control-plane instances sharing one database, with automatic leader election
and failover, and cross-instance task notification. Still ahead: a future
SDK for running arbitrary user code as a task. See `MIRANDA_ROADMAP.md` for
exactly what's been run for real versus what's merely compiled.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you shall be dual licensed as
above, without any additional terms or conditions.
