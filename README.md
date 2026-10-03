# runtime

`runtime` was originally built inside [Flux](https://github.com/odunboye/flux)
(as `flux-runtime`, itself an outright rename of the package's very first
name, `flux-async`) and later moved back out to its own repo, since it has
no Flux-specific dependencies. The module prefix changed from `Flux.Async.*`/
`Flux.Stream.*` to `Async.*`/`Stream.*` as part of that move.

An owned-task runtime for Idris2 on Chez, targeting macOS and Linux. It
uses `elin` for typed errors and a small C shim for nonblocking sockets,
readiness polling, monotonic time, and opt-in standalone signal supervision.
It does not depend on `async`, `async-posix`, `streams`, or `streams-posix`.

## Build

Install Pack and a C compiler, then run `pack build runtime.ipkg`. The
package prebuild compiles the native library; Pack installs it alongside the
Idris package so downstream executables can copy it into their app directory.

```idris
import Async.Core
import Async.Runner

main : IO ()
main = do
  result <- runTask $ the (Task [] Nat) $ do
    child <- spawn (the (Task [] Nat) (sleep 10 >> pure 42))
    fromOutcome !(join child)
  case result of
    Right (Succeeded n) => printLn n
    Left error => putStrLn error
    _ => putStrLn "task did not succeed"
```

`Task es a` tracks typed errors. `sleep` takes milliseconds. `bracket` masks
acquisition and cleanup, restores cancellation during use, and joins children
and blocking jobs before releasing the resource. `cancel` joins; `race` cancels
and joins its loser. Neither abandons a non-cooperative IO operation.

`startRuntime n` creates fixed owner loops. `submit` distributes roots round
robin; their descendants and continuations stay on that owner. `submitOwned`
transfers a resource with its finalizer already installed, including when
cancellation arrives before the body starts. Rejected admission leaves the
resource with the caller. Each mailbox holds at most 128 pending roots.

Blocking IO uses four shared workers and a queue of 128 jobs. Idle workers
wait on condition variables; native blocking calls must allow Chez garbage
collection. `liftIO` is for short synchronous operations, not database reads
or other potentially blocking work.

Socket descriptors have one owner thread. A socket permits one reader and
one writer concurrently; competing operations in the same direction fail
with `SocketBusy`. Close sockets only after their users have joined. The
server supervisor bounds active connections, transfers accepted peers to
owners, drains on a stop request, then cancels outstanding connections.

`requestDrain` stops admission while letting existing tasks finish.
`requestShutdown` also requests cancellation. Observe `hasStopped` before
considering resources reclaimed. Library APIs never force process exit.
`Async.Standalone` explicitly opts into process-wide SIGINT/SIGTERM
handlers and an independent native watchdog; Flux's standalone runner gives
requests 30 seconds to drain and cleanup another 5 seconds before exit 124.

## Streams

`Stream` supplies `Pull Task`, typed error handling, chunk/byte
combinators, and scoped resources. Early termination and caught errors close
resources before their continuation runs. A scope cancels and joins children
spawned by stream effects before releasing resources. Finished hooks and
children are pruned from long-lived scopes.

`Stream.Socket` supplies socket input/output, and `Stream.Posix`
reads regular files on blocking workers. Pure combinators were adapted from
Stefan Hoeck's idris2-streams; see `STREAMS_LICENSE`. The evaluator and its
scope ownership are independent of that library's scheduler.

## Verification

From this directory:

```sh
make -f test/Makefile.native check
pack build test/test.ipkg
pack build test/service.ipkg
pack build test/stream.ipkg
pack build test/socket.ipkg
./test/build/exec/flux-async-test
./test/build/exec/flux-async-service-test
./test/build/exec/flux-async-stream-test
python3 test/socket_test.py
```

Run executables under external deadlines in CI. Coverage includes typed
errors, deep binds, cancellation during acquisition, pre-start ownership
transfer, thread affinity, worker joining, drain escalation, stream child
ownership, binary backpressure, native interrupted waits, and forced exit.
The downstream Flux tests exercise real HTTP framing and shutdown. The
extended soak is `python3 test/runtime_soak.py --seconds 7200` in Flux.

The runtime, service, and stream suites pass on macOS arm64 and Linux amd64
(the latter in a local Docker image under emulation). Native tests pass with
ASan/UBSan on both platforms. `test/linux_runtime.sh` builds the Idris tests
offline from read-only source mounts; `test/linux_native.sh` tests the C shim
and PG transport. End-to-end Flux/PG and the extended HTTP soak were run on
macOS; those results do not establish Linux application performance.
