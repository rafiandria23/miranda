# Miranda crates

Ten crates, split by what actually needs to depend on what. This doc is the
map for "I opened `crates/`, now what am I looking at."

## Where to start, by task

- **Changing what a workflow *is*** (a task, a dependency, an execution's
  state machine)? `core`.
- **Changing how a task actually runs** (`shell`, `http`, `wait`, `noop`, or
  adding a new one)? `worker`.
- **Changing how workflows/state are persisted**, or adding a new database
  backend? `storage`, then the matching `storage-<engine>` crate.
- **Changing single-process ("embedded") execution**? `engine`.
- **Changing distributed coordination** (leases, the task queue, routing,
  leader election, worker registration)? `control-plane`.
- **Changing the `miranda-server` binary** (gRPC, HTTP API, process
  startup)? `server`.
- **Changing the `miranda-cli` binary** (`run`, `submit`, `status`, `db`)?
  `cli`.

## `core`

```
core/src/
├── lib.rs
├── id.rs             # every ID type (WorkflowId, ExecutionId, WorkerId, ...)
├── workflow.rs        # WorkflowDefinition, WorkflowTask
├── execution.rs        # Execution, TaskStatus
├── event.rs             # Event, EventPayload
├── queue.rs               # QueuedTask (persistent queue-row shape)
├── router.rs                # WorkerRegistration (persistent worker-row shape)
└── lease.rs                   # Lease (persistent, OffsetDateTime-based lease row)
```

No I/O, no database, no network. Everything else in the workspace depends
on `core`; `core` depends on nothing in this workspace. The standing rule:
**`core` owns persistent entity definitions** (things a row in a database
represents); other crates own their own runtime-only types (an in-memory
worker's own bookkeeping struct, for instance, is not a `core` type even
if it looks similar).

## `worker`

```
worker/src/
├── lib.rs
├── error.rs             # WorkerError
├── assignment.rs          # TaskAssignment, ControlPlaneClient trait
├── executor.rs               # TaskExecutor trait, DispatchExecutor
├── heartbeat.rs                 # HeartbeatRunner
└── worker.rs                       # Worker, WorkerConfig, WorkerHandle, the poll/heartbeat/execute loop
```

Built-in executors (`ShellExecutor`, `HttpExecutor`, `WaitExecutor`,
`NoopExecutor` or similar) live under here too — check `executor.rs` or an
adjacent `executor/` folder for the current shape. New built-in task types
land in this crate.

## `storage`

```
storage/src/
├── lib.rs
├── error.rs                       # StorageError
├── workflow_store.rs                 # WorkflowStore trait
├── task_queue_store.rs                  # TaskQueueStore trait
├── router_store.rs                         # RouterStore trait
├── lease_store.rs                             # LeaseStore trait
├── join_token_store.rs                           # JoinTokenStore trait
├── leadership_store.rs                              # LeadershipStore trait
├── peer_store.rs                                       # PeerStore trait
└── memory.rs                                              # InMemoryStore — implements every trait above
```

Every trait here is deliberately storage-shaped and backend-agnostic — no
knowledge of `control-plane`'s vocabulary, no assumption about which SQL
engine (if any) implements it. `InMemoryStore` is the one shared
implementation used by embedded mode and by tests.

## `storage-postgres`, `storage-mysql`, `storage-sqlite`

```
storage-<engine>/
├── migrations/
│   ├── 0001_create_workflows_table.sql
│   ├── 0002_create_task_queue_table.sql
│   ├── 0003_create_workers_table.sql
│   ├── 0004_create_leases_table.sql
│   ├── 0005_create_join_tokens_table.sql
│   ├── 0006_create_leadership_table.sql
│   └── 0007_create_control_plane_instances_table.sql
└── src/
    ├── lib.rs
    ├── config.rs          # <Engine>Config (connection settings)
    └── store.rs               # <Engine>Store — implements every storage/ trait
```

One crate per supported database, each implementing every trait from
`storage` against that engine's real dialect. Each depends **only** on
`storage`, never on `control-plane` — this boundary is load-bearing, not
incidental (see "Deliberately unresolved / parked" in
`MIRANDA_ARCHITECTURE.md` for the full reasoning). Migration filenames are
numbered and named for what they do (`sqlx-cli`/ecosystem convention:
`NNNN_verb_object.sql`), never edited in place once applied to a real,
running database — a schema change becomes a new, later-numbered file.

Adding a new database engine means a new `storage-<engine>` crate
implementing the same trait set; nothing else in the workspace needs to
change.

## `engine`

```
engine/src/
├── lib.rs
├── error.rs           # EngineError
├── retry.rs              # RetryPolicy
└── task_runner.rs           # TaskOutcome, TaskOutcomeResult
```

Embedded-mode orchestration — `EmbeddedEngine` drives a
`WorkflowDefinition` directly against a `TaskExecutor`, one process, no
network. This is what `miranda run` uses.

## `control-plane`

```
control-plane/src/
├── lib.rs
├── error.rs                    # ControlPlaneError
├── control_plane.rs               # ControlPlane<Q, R, S, D, N, L> itself
├── dispatcher.rs                     # DispatchStrategy trait
├── dispatcher/
│   └── routed.rs                        # RoutedDispatcher
├── queue.rs                                # TaskQueue trait
├── queue/
│   ├── memory.rs                              # InMemoryTaskQueue
│   └── durable.rs                                # DurableTaskQueue<S>
├── router.rs                                        # Router trait
├── router/
│   ├── memory.rs                                        # InMemoryRouter
│   └── durable.rs                                          # DurableRouter<S>
├── notifier.rs                                                # TaskNotifier trait, NullTaskNotifier
├── lease_manager.rs                                              # LeaseManager<S>
└── leadership.rs                                                    # LeadershipRunner<S>
```

Defines *what* distributed coordination needs — leases, routing,
notification, leadership, dispatch — as traits, without knowing *how* any
of it is transported. No `tonic`, no gRPC dependency in this crate, on
purpose. `server` is where these abstractions get a real, wire-level
implementation.

## `server`

```
server/
├── build.rs                # compiles proto/miranda/{worker,control_plane}/v1/*.proto
└── src/
    ├── main.rs
    ├── cli.rs                  # Cli (clap) — every --flag
    ├── bootstrap.rs               # build_control_plane, run_control_plane_only/run_worker_only/run_colocated
    ├── local_client.rs               # LocalControlPlaneClient (co-located worker <-> control plane, in-process)
    ├── http.rs                          # wiring only
    ├── http/
    │   ├── error.rs                        # HttpError
    │   └── router.rs                          # composes per-resource routers, attaches state once
    ├── http/router/
    │   ├── workflows.rs                          # POST /workflows
    │   └── executions.rs                            # POST /executions, GET /executions/{id}
    ├── grpc.rs                                          # wiring only, serve_all (both services, one port)
    ├── grpc/
    │   ├── error.rs                                        # to_status — shared across every gRPC service
    │   ├── worker_service.rs                                  # wiring only
    │   ├── worker_service/
    │   │   ├── service.rs                                        # WorkerServiceImpl (worker-facing RPCs)
    │   │   ├── client.rs                                            # RemoteControlPlaneClient
    │   │   └── notifier.rs                                             # GrpcTaskNotifier
    │   ├── control_plane_service.rs                                       # wiring only
    │   └── control_plane_service/
    │       ├── service.rs                                                    # ControlPlaneServiceImpl (peer-facing)
    │       ├── client.rs                                                        # PeerClient
    │       └── peers.rs                                                            # PeerManager<S>
```

This is where `control-plane`'s abstract traits meet real network code —
`GrpcTaskNotifier`, `PeerManager`, `RemoteControlPlaneClient`, all live
here, not in `control-plane`, on purpose (same reason `control-plane`
stays free of a `tonic` dependency). `grpc/worker_service/` and
`grpc/control_plane_service/` are structural siblings — one service per
folder, `service.rs`/`client.rs` split inside each — the pattern to follow
if a third gRPC service is ever added.

## `cli`

```
cli/src/
├── main.rs
├── run.rs           # embedded execution
├── submit.rs           # POST /executions against a running server
├── status.rs               # GET /executions/{id}
├── register.rs                # POST /workflows
└── db.rs                         # storage setup/inspection
```

## A few conventions worth knowing before you dig in

- **Trait shape**: a storage-primitive or domain trait that needs to
  become an `Arc<dyn Trait>` trait object elsewhere uses `Pin<Box<dyn
  Future<...> + Send + 'a>>` return types, not `impl Future` sugar or
  `async fn` in a trait. A trait only ever used as a plain generic bound
  (never boxed) is fine with `impl Future` — check how the trait actually
  gets consumed downstream before picking a shape.
- **`time::Duration` vs `std::time::Duration`**: wall-clock spans and
  moments use `time::Duration`/`time::OffsetDateTime` throughout.
  `std::time::Duration` is kept only where `std`/`tokio` genuinely require
  it (`tokio::time::interval`/`sleep`/`timeout`), converted once at the
  call site that needs it (`.try_into().expect(...)`), never threaded
  through as the ambient type.
- **Generic `<S>` wrappers store `S` directly, never `Arc<S>`.** Every
  real caller already hands in an already-`Arc`-erased trait object
  (`Arc<dyn LeaseStore>`, etc.); wrapping it in a second `Arc` inside the
  struct is a real, easy mistake — it surfaces as a confusing `Sized`
  compiler error, not an obviously-related one, and it's happened more
  than once.
- **Blanket `impl Trait for Arc<dyn Trait>`** is needed exactly when, and
  only when, `Arc<dyn Trait>` gets handed in as the concrete
  instantiation of some *other* type's own `<S: Trait>` generic parameter
  — never for a trait object used as a plain field or parameter type on
  its own. Whether the `+ '_'` lifetime is needed on that blanket impl
  isn't predictable from the trait's shape; confirm by building, not by
  analogy to a similar-looking trait.

See `MIRANDA_ARCHITECTURE.md` (repo root) for the full reasoning behind
these, `MIRANDA_ROADMAP.md` for what's actually been built and verified
versus merely designed, and `../proto/README.md` for the `.proto` layout
`server`'s `build.rs`/`grpc/` folders compile against.
