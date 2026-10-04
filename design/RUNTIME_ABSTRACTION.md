# Flux's connection-supervision boundary

**Moved here from [odunboye/flux](https://github.com/odunboye/flux)'s
`design/` folder** - this design sketch is about this package's own
scheduler/supervision architecture (from when it was vendored inside Flux as
`flux-runtime`/`flux-async`), not about Flux itself, so it belongs here now
that this package has its own repo. Paths below (`libs/idris2-flux-async`,
etc.) are historical and reflect that earlier vendored location, not this
repo's current layout.

Status: **historical design sketch, superseded by the owned runtime** in
`libs/idris2-flux-async`. See [RUNTIME_IMPLEMENTATION.md](RUNTIME_IMPLEMENTATION.md)
for the implemented migration and its verification. The mechanism and source
line references below describe the earlier implementation. Revision 2 - revision 1 proposed a general `Runtime m`
typeclass replacing `Async Poll [...]` throughout Flux; an external
review checked it against the real source and found it both broader
than necessary and, in its `race`/`cancelTask` contract, not actually a
fix for the bug it targeted. Corrections below are folded in, verified
against source, not taken on faith. See the bottom of this file for the
verification trail if you want to check the checking.

## Why this exists

See `libs/idris2-async/INVESTIGATION_NOTES.md` for the underlying bug.
Short version: idris2-async's round-robin scheduler fix is validated
and real, but shipping it is blocked on a load-dependent stall in
`serveConnections`'s bounded shutdown drain (`Flux.Core.HTTP.idr:128-133`).

## The actual mechanism (not a hypothesis - traced through source)

```idris
-- HTTP.idr:122-138
serveConnections maxOpen sink outer = do
  available <- semaphore maxOpen
  finally
    (race_ [acquireN available maxOpen, sleep drainTimeout])
    (foreach (run available) outer)
  where
    run available v = do
      acquire available
      ignore $ start (guarantee (sink v) (release available))
```

`race_` is `race ()` (`Util.idr:231-232`), which bottoms out in `race2`
(`Util.idr:195-210`):

```idris
race2 fa fb ac bc dflt =
  uncancelable $ \poll => poll (racePair fa fb) >>= \case
    Left  (oc,f) => case oc of
      Succeeded res => cancel f $> ac res
      ...
```

**`cancel f` is called on the loser regardless of which side "won," and
its own definition (`Util.idr:60`) is `uncancelable $ \_ => runIO
f.cancel_ >> ignore (join f)` - it waits for the loser to actually
terminate before returning.** So when `sleep drainTimeout` wins,
`race_` still blocks on `acquireN`'s cancellation *fully completing*
before it returns to `finally`. There is no separate bound on that
wait. If a connection's fiber is slow to observe cancellation under
load - exactly the scenario a sustained-traffic benchmark would hit -
`race_` hangs past `drainTimeout`, which is precisely the observed
symptom (2/7 trials, 60s+ instead of ~30s).

**This is the central correction from revision 1.** That draft's
`cancelTask`/`race` contract ("must not return until cancellation has
been observed") wasn't a fix - it's what `cancel`/`race2` *already do*.
Restating it as a new interface's contract would have reproduced the
exact bug behind a different name. A race against a deadline that
itself waits for the loser's cancellation cannot provide a hard upper
bound; a hard deadline requires being allowed to return at the deadline
*without* waiting for the loser, which is a materially different
operation from what `race_`/`cancel` give you today.

Separately, `run`'s two steps - `acquire available` then `start
(guarantee (sink v) (release available))` - are not atomic. A
cancellation of the *accepting* fiber between them leaks the permit
forever: it was acquired, but no child fiber exists yet whose
`guarantee` would ever release it. This is a real window in the current
code, not a hypothetical the review raised for its own sake.

## Scope: narrower than revision 1

Revision 1 proposed replacing `Async Poll [Errno,HTTPErr]` broadly.
Two things wrong with that as a first step:

1. **The coupling is wider than "one file."** `Flux.Core.Middleware`'s
   `AppProg = Async Poll [Errno,AppError]` (`Middleware.idr:225-226`)
   depends on the same concrete types via `HTTP.idr`'s `import public
   IO.Async.Loop.Posix` - no direct `import IO.Async...` line of its
   own, so a grep for direct imports understates it. `AppError`
   (`Middleware.idr:215-216`, `{status: Nat, message: String}`) is also
   a genuinely different type from `HTTPErr` (`HTTP.idr:212-217`, a
   4-constructor enum) - revision 1's single `FluxError` sum type
   didn't account for `AppError` at all, so it didn't actually cover
   the handler layer.
2. **`streams-posix`'s `AsyncPull`/`AsyncStream` is a separate,
   large dependency** (`HTTPPull`/`HTTPStream`/`Responder` all built on
   it) that a from-scratch alternative runtime could not directly run
   against without its own, much bigger migration.

Given both, the first revision should **preserve** `Async`, the
existing error lists, the streaming types, and the public handler
types entirely - and introduce one injectable boundary around the
specific thing that's actually broken: admission, child lifetime, and
shutdown draining. This is also just more useful on its own terms -
`serveConnections` is the one place in Flux with a documented,
reproducible reliability gap; the rest of the framework isn't waiting
on a runtime swap to be trustworthy.

## The boundary

```idris
module Flux.Supervisor

||| One admitted connection's slot. Returned by `admit`, consumed by
||| exactly one `superviseChild` call.
export
data AdmitTicket : Type where
  MkTicket : (slot : Nat) -> AdmitTicket

public export
data DrainResult = AllDrained | DeadlineElapsed (stillRunning : Nat)

||| Supervises admission, child lifetime, and shutdown draining for a
||| connection-accepting loop - the behavior `serveConnections`
||| currently hardcodes inline (HTTP.idr:122-138). Preserves `Async e
||| es`, existing error lists, streams, and handler types entirely -
||| this abstracts ONE policy, not the runtime.
public export
interface ConnectionSupervisor (e : Type) where
  ||| Blocks (via ordinary, cooperative cancellation - no special
  ||| contract here) until a slot is available under the configured
  ||| `maxOpen`.
  admit : Async e [] AdmitTicket

  ||| Hands `child`'s lifetime to the supervisor as `ticket`'s owner,
  ||| ATOMICALLY with the ticket itself - no window exists between
  ||| "slot acquired" and "something owns releasing it" the way
  ||| `run`'s `acquire`-then-`start` does today. `child` runs to
  ||| completion (or cancellation) with its slot guaranteed to be
  ||| released exactly once, regardless of outcome.
  superviseChild : AdmitTicket -> Async e es () -> Async e fs ()

  ||| Waits for every admitted child to finish, OR `deadline` elapses -
  ||| whichever comes first, and is ALLOWED to return at the deadline
  ||| even if children haven't acknowledged cancellation yet. This is
  ||| the actual fix: it does not internally use a `race_`/`cancel`
  ||| pairing whose return is gated on the loser's cancellation
  ||| completing (see "the actual mechanism" above for why that
  ||| specifically doesn't provide a hard bound). Concretely, this
  ||| polls admitted-slot count against a wall clock rather than racing
  ||| against a fiber it then has to wait to cancel - still-running
  ||| children are reported via `stillRunning`, not silently joined.
  ||| What happens to them after `DeadlineElapsed` (best-effort
  ||| detached cancellation? left for process exit to reclaim?) is a
  ||| decision this interface makes a caller aware of, not one it
  ||| hides.
  drain : (deadline : Clock Duration) -> Async e [] DrainResult
```

`serveConnections` becomes:

```idris
serveConnections : ConnectionSupervisor e => AsyncPull e o es r -> AsyncPull e q es r
serveConnections outer = do
  finally
    (drain drainTimeout)
    (foreach runChild outer)
  where
    runChild : o -> Async e es ()
    runChild v = do
      ticket <- admit
      superviseChild ticket (sink v)
```

The existing behavior (semaphore-backed, `race_`-based) becomes the
default `ConnectionSupervisor` implementation, unchanged in
characteristics - same bug exposure - just named and isolated instead
of inline. That alone is worth doing before anything else: it turns
"read `race2`'s cancellation semantics to understand the drain" into
"read one interface," and it's what makes a *second* implementation
(one whose `drain` genuinely bounds its own wait) something you can
write and test without touching `HTTP.idr`'s other ~900 lines.

## What "specify the guarantees, then test" means concretely

Before writing a second implementation:

1. Write `drain`'s contract as an actual property, not prose: *for any
   deadline `d` and any set of admitted children (including ones that
   never terminate), `drain d` returns within `d + ε` for a fixed,
   documented `ε`* - and pick whether `ε` is allowed to depend on the
   number of children or must be constant.
2. Same for `superviseChild`: *a ticket is released exactly once, and a
   cancellation of the admitting fiber at any point - including between
   `admit` and `superviseChild` - does not leak it.*
3. Run the existing (`race_`-based) implementation against both under
   the same sustained-load conditions that found the original stall,
   confirming it actually violates them (not just suspected to).
4. Only then is there a real target to prototype an alternative
   `drain` against - e.g., one that polls the semaphore's count on a
   timer instead of racing a cancelable wait, or one that uses a
   separate reclamation path for `DeadlineElapsed` children instead of
   joining them.

## Verification trail (for the corrections above)

- `cancel`'s real definition and doc comment: `libs/idris2-async/src/IO/Async/Util.idr:60`
- `race2`'s unconditional `cancel f` before returning: same file, `:195-210`
- `race_`/`race`: same file, `:223-232`
- `serveConnections`/`run`, the exact current logic: `projects/flux/src/Flux/Core/HTTP.idr:122-138`
- `Middleware.AppProg` also pinned to `Async Poll`, via re-export not direct import: `projects/flux/src/Flux/Core/Middleware.idr:225-226`
- `AppError` vs `HTTPErr` as genuinely distinct types: `Middleware.idr:215-216`, `HTTP.idr:212-217`
- `Semaphore.release` (fixed to 1) vs `releaseN` (takes a count): `libs/idris2-async/src/IO/Async/Semaphore.idr:62-70`
