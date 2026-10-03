module Async.Core

import public Control.Monad.MCancel
import public Data.Linear.ELift1

%default total

public export
data BlockingError = BlockingQueueFull | BlockingPoolClosed

public export
data ReadinessError = RegistrationFailed | PollFailed

||| Observation and cancellation are synchronized by the runtime. A handle
||| does not grant access to the task's owner-thread interpreter state.
public export
record Fiber (es : List Type) a where
  constructor MkFiber
  observe : IO (Maybe (Outcome es a))
  requestCancel : IO ()

||| Instructions, interpreted one reduction at a time. No dependency on
||| idris2-async, its scheduler, semaphore, or stream scopes.
public export
data Task : List Type -> Type -> Type where
  Value : a -> Task es a
  Raise : HSum es -> Task es a
  Bind : Task es a -> (a -> Task es b) -> Task es b
  Attempt : Task es a -> Task fs (Result es a)
  Sync : IO a -> Task es a
  Blocking : IO a -> Task es (Either BlockingError a)
  AwaitFD : Int -> Int -> Task es (Either ReadinessError Int)
  Pause : Nat -> Task es ()
  Cede : Task es ()
  CancelSelf : Task es a
  Start : Task es a -> Task fs (Fiber es a)
  Join : Fiber es a -> Task fs (Outcome es a)
  RequestCancel : Fiber es a -> Task fs ()
  Finally : Task es a -> Task [] () -> Task es a
  Bracket : Task es r -> (r -> Task [] ()) -> (r -> Task es a) -> Task es a
  Interruptible : Task es a -> Task es a
  Masked : Task es a -> Task es a
  -- Stream evaluator primitive: transfer spawned children to the caller and
  -- return join handles so a stream scope can close them before its resources.
  CaptureChildren : Task es a -> Task es (a, List (Fiber [] ()))

public export
MErr Task where
  fail = Raise
  succeed = Value
  attempt = Attempt
  bind = Bind
  mapImpl f t = Bind t (Value . f)
  appImpl f x = Bind f (\g => Bind x (Value . g))

export
ELift1 World Task where
  elift1 act = Bind (Sync (runIO (\t => toResult (act t)))) (either Raise Value)

export
HasIO (Task es) where
  liftIO = Sync

export
liftIO : IO a -> Task es a
liftIO = Sync

||| Submit to the bounded worker pool. Cancellation does not kill native IO;
||| the enclosing scope retains ownership until that job actually finishes.
export
blocking : IO a -> Task es (Either BlockingError a)
blocking = Blocking

||| Internal transport primitive: 1 = readable, 2 = writable. Registrations
||| belong to the task's loop and are removed before completion/cancellation.
export
awaitFD : Int -> Int -> Task es (Either ReadinessError Int)
awaitFD = AwaitFD

export
sleep : Nat -> Task es ()
sleep = Pause

export
yield : Task es ()
yield = Cede

export
spawn : Task es a -> Task fs (Fiber es a)
spawn = Start

export
join : Fiber es a -> Task fs (Outcome es a)
join = Join

export
cancel : Fiber es a -> Task fs ()
cancel f = RequestCancel f >> ignore (Join f)

export
guarantee : Task es a -> Task [] () -> Task es a
guarantee = Finally

export
bracket : Task es r -> (r -> Task [] ()) -> (r -> Task es a) -> Task es a
bracket = Bracket

export
fromOutcome : Outcome es a -> Task es a
fromOutcome (Succeeded a) = pure a
fromOutcome (Error e) = Raise e
fromOutcome Canceled = CancelSelf

-- This polls synchronized completion cells, not task interpreter state.
-- The runner's timer queue suspends the caller between observations.
covering
first : Fiber es a -> Fiber es b -> Task es (Either a b)
first a b = do
  ma <- liftIO a.observe
  mb <- liftIO b.observe
  case (ma, mb) of
    (Just oa, _) => cancel b >> map Left (fromOutcome oa)
    (_, Just ob) => cancel a >> map Right (fromOutcome ob)
    _ => sleep 1 >> first a b

||| Cancels and joins the loser, including its scoped children/finalizers.
||| This is deliberately not a hard timeout for non-cooperative IO.
export covering
race : Task es a -> Task es b -> Task es (Either a b)
race a b = do
  fa <- spawn a
  fb <- spawn b
  guarantee (first fa fb) (cancel fa >> cancel fb)
