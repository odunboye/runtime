# Owned runtime implementation checkpoint — 2026-09-11

**Moved here from [odunboye/flux](https://github.com/odunboye/flux)'s
`design/` folder** - this checkpoint is about this package's own
implementation history (from when it was vendored inside Flux as
`flux-runtime`/`flux-async`), not about Flux itself, so it belongs here now
that this package has its own repo.

**The `test/reports/` paths this checkpoint cites as evidence (`runtime-checkpoint-2026-09-11/`,
`runtime-completion/`) lived in Flux's own repo and were later removed there
as disposable test-run output** - the narrative and numbers below are kept
as the historical record of this checkpoint, not a live link; `python3
tools/workspace.py test` was Flux's own verification command, run against
this package as a dependency, not a command in this repo.

Flux now uses the independent `flux-async` package in
`libs/idris2-flux-async`. Its dependencies no longer include `async`,
`async-posix`, `streams`, or `streams-posix`. Typed error lists are preserved.
This supersedes the historical proposal in `RUNTIME_ABSTRACTION.md`.

## Implemented

- Fixed owner loops, round-robin root admission, owner-affine children and
  continuations, native readiness polling, timers, and scoped cancellation.
- Four blocking workers with a bounded queue of 128 jobs. Idle workers use
  condition variables so they do not obstruct Chez garbage collection.
- Masked resource acquisition/release, child and worker joining before release,
  and cleanup installed before accepted socket ownership transfers.
- Independent stream evaluator with cleanup on early termination and caught
  failure; stream children stop before their resources close.
- Nonblocking sockets with backpressure and one operation per direction.
- Standalone shutdown: stop admission, drain for 30 seconds, cancel and join;
  an independent native watchdog exits 124 around 35 seconds after a signal
  if cleanup stalls. Library APIs never force process exit.
- Native PG transport deadlines, no abandoned query threads, and rejection of
  further operations on timed-out or protocol-damaged connections.
- `postgres-async` exclusive pool: defaults of eight connections, 128 waiters,
  and a five-second acquisition deadline. Cold authentication is serialized
  while other borrowers remain eligible to reuse returned connections.
- `Flux.DB.Pool` pooled repositories and a whole-transaction lease helper;
  `Flux.DB.PG.dbIO` offloads operations. Todo-api uses the pool.

## Verified at this checkpoint

| Check | Result |
| --- | --- |
| Runtime / service / stream suites | 8 / 16 / 7 checks pass on macOS arm64 and Linux amd64 |
| Native runtime and PG transport | ASan/UBSan checks pass on macOS and Linux |
| Socket binary transfer / backpressure / accept cancellation | Pass on macOS |
| Flux full regression suite and HTTP property checks | Pass |
| Live HTTP framing and shutdown probes | Pass |
| PG unit and real-database integration suites | Pass |
| Isolated TLS transport test | Encrypted query, timeout poisoning, and reuse rejection pass |
| PG pool suite | 15 checks pass, including queue overflow and acquisition timeout recovery |
| Flux DB integration suite | Pass, including cross-repository commit and rollback |
| Todo-api raw PG / in-memory / pooled suites | 26 / 20 / 24 checks pass |
| Live pooled todo-api, four owners | 24 clients, 480 CRUD lifecycles, 3,360 checked requests; clean exit |

The two-hour macOS HTTP soak completed three 40-minute phases with 24
keep-alive clients. Each response was checked for status and exact body.

| Owners | Requests | Errors | Shutdown exit |
| --- | ---: | ---: | ---: |
| 1 | 33,928,233 | 0 | 0 |
| 2 | 38,855,711 | 0 | 0 |
| 4 | 34,581,660 | 0 | 0 |

Total: **107,365,604 requests**. RSS samples and results are committed in
`test/reports/runtime-checkpoint-2026-09-11/`. This is a Python-generated
loopback endurance workload, not a maximum-throughput benchmark. Other
verification work ran on the same machine during the soak.

## Outstanding verification and limits

- Follow-up verification closed the Linux socket and macOS one-owner live
  application gaps. Linux runtime/service/stream suites and macOS Flux
  regressions and live protocol probes also passed again. See
  `../test/reports/runtime-completion/README.md` for commands and evidence.
- The follow-up Linux full-stack timeout failure is resolved in `idris2-pg`
  commit `2e22c85078578d8128fecf1a489d700f7675fb15` by optimizing pure-Idris
  SHA-256 list traversal. SCRAM authentication
  previously took about 2.6 seconds per connection in Docker emulation;
  serialized cold connection setup consumed queued borrowers' deadlines.
  No deadline, SCRAM iteration count, or pool setting was relaxed.
- Final regression drivers exit 0 on macOS arm64 and Docker Linux amd64
  (emulated). Both pass 163 PG unit checks, crypto properties, PG integration,
  runtime/service/stream/socket suites, native sanitizer checks, Flux regressions,
  protocol probes, 24 pooled todo-api checks, and live 1/2/4-owner workloads
  (3,360 checked HTTP requests per owner configuration; clean shutdown).
  macOS additionally passes the 15-check pool suite, isolated TLS transport
  tests, and Flux DB integration. Linux native sanitizers disable leak detection
  under emulation; the ordinary Linux PG integration TLS case skips because
  that test database has SSL disabled. See `../test/reports/runtime-completion/final/`.
- The recorded two-hour soak predates the SHA-256 optimization; it has not
  been repeated after that change. Native-hardware Linux performance and
  production readiness are not established by these Docker checks.
- Cancellation joins arbitrary IO; it cannot preempt arbitrary native calls,
  DNS resolution, or CPU work. Pool acquisition can exceed its nominal
  deadline while owned native work finishes; resources are not abandoned.
- Pool callbacks must not retain or fork use of the borrowed DB. A raw DB
  must not be shared concurrently. Connections left in a transaction are
  discarded before reuse.
- PG TLS certificate/hostname verification remains a pre-existing gap,
  outside this runtime migration. The TLS transport test does not establish
  authenticated TLS security.
- The compiler still reports the pre-existing `Helper.unimplementedEncode`
  hole. The exercised code paths do not invoke it.

This is a tested development checkpoint, not a declaration of production
readiness. Package READMEs and the test scripts contain reproduction commands.

## Companion repository commits

- `libs/idris2-flux-async`: `bfc3ff67c3d4dd85462ba7063d6d81ad7328ccf5`
- `libs/idris2-pg`: `cc7fb1a43e26247a9626c6ee8bdc8c8e5cde125a`
- `libs/nebula`: `15cb3939ce2a3420418e16b855c6db5d5c7bf7bb`
- `libs/nebula-flux`: `51f0c818f3a86c2fde4fb59e432b69b949d923e5`
- `playground/todo-api`: `731fbd4f6942d7073c8fc91b10da53b752c8a002`
