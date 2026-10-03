module Async.Runner

import Async.Core
import Async.Native
import Data.IORef
import Data.List
import System
import System.Clock
import System.Concurrency

%default covering

-- Only these small cells cross threads. All interpreter continuations,
-- child lists, and runnable queues remain on their owner thread.
record Cell a where
  constructor MkCell
  mutex : Mutex
  value : IORef a

newCell : a -> IO (Cell a)
newCell v = MkCell <$> makeMutex <*> newIORef v

readCell : Cell a -> IO a
readCell c = do
  mutexAcquire c.mutex
  v <- readIORef c.value
  mutexRelease c.mutex
  pure v

writeCell : Cell a -> a -> IO ()
writeCell c v = do
  mutexAcquire c.mutex
  writeIORef c.value v
  mutexRelease c.mutex

modifyCell : Cell a -> (a -> (a, b)) -> IO b
modifyCell c f = do
  mutexAcquire c.mutex
  old <- readIORef c.value
  let (next, result) = f old
  writeIORef c.value next
  mutexRelease c.mutex
  pure result

nowMs : IO Integer
nowMs = do
  t <- clockTime Monotonic
  pure (seconds t * 1000 + nanoseconds t `div` 1000000)

data Step = Done | Ready (IO Step) | Sleeping Integer Bool (IO Step)
          | Waiting Int64 Bool (Either ReadinessError Int -> IO Step)

record Workers where
  constructor MkWorkers
  queue : Cell (Bool, List (IO ()))
  available : Condition
  exited : List (Cell Bool)

takeWork : Workers -> IO (Maybe (IO ()))
takeWork pool = do
  mutexAcquire pool.queue.mutex
  takeLocked
  where
    takeLocked : IO (Maybe (IO ()))
    takeLocked = do
      (admitting, jobs) <- readIORef pool.queue.value
      case jobs of
        job :: rest => do
          writeIORef pool.queue.value (admitting, rest)
          mutexRelease pool.queue.mutex
          pure (Just job)
        [] => if not admitting
          then mutexRelease pool.queue.mutex $> Nothing
          else conditionWait pool.available pool.queue.mutex >> takeLocked

worker : Workers -> Cell Bool -> IO ()
worker pool done = do
  job <- takeWork pool
  case job of
    Just action => action >> worker pool done
    Nothing => writeCell done True

newWorkers : IO Workers
newWorkers = do
  queue <- newCell (True, [])
  exited <- traverse (\_ => newCell False) (replicate 4 ())
  available <- makeCondition
  let pool = MkWorkers queue available exited
  traverse_ (\done => ignore (fork (worker pool done))) exited
  pure pool

stopWorkers : Workers -> IO ()
stopWorkers pool = do
  mutexAcquire pool.queue.mutex
  (_, jobs) <- readIORef pool.queue.value
  writeIORef pool.queue.value (False, jobs)
  conditionBroadcast pool.available
  mutexRelease pool.queue.mutex

awaitWorkers : Workers -> IO ()
awaitWorkers pool = do
  stopped <- traverse readCell pool.exited
  unless (all id stopped) (usleep 1000 >> awaitWorkers pool)

enqueueWork : Workers -> IO () -> IO (Either BlockingError ())
enqueueWork pool job = do
  mutexAcquire pool.queue.mutex
  (admitting, jobs) <- readIORef pool.queue.value
  result <- if not admitting then pure (Left BlockingPoolClosed)
    else if length jobs >= 128 then pure (Left BlockingQueueFull)
    else do
      writeIORef pool.queue.value (admitting, jobs ++ [job])
      conditionSignal pool.available
      pure (Right ())
  mutexRelease pool.queue.mutex
  pure result

record Control where
  constructor MkControl
  canceled : Cell Bool
  complete : Cell Bool
  children : IORef (List Control)
  blockingJobs : IORef (List (Cell Bool))
  notify : IO ()

record Job where
  constructor MkJob
  control : Control
  step : Step

record Loop where
  constructor MkLoop
  pending : IORef (List Job)
  inbox : Cell (Bool, List Job)
  exited : Cell Bool
  cancelRequested : Cell Bool
  workers : Workers
  poller : AnyPtr
  wakeHandle : Cell (Maybe AnyPtr)
  nextToken : IORef Int64

newLoop : Workers -> IO (Either String Loop)
newLoop pool = do
  p <- primIO primNew
  if prim__nullAnyPtr p /= 0
    then pure (Left "could not allocate native poller")
    else map Right $ MkLoop <$> newIORef [] <*> newCell (True, []) <*> newCell False
                <*> newCell False <*> pure pool <*> pure p <*> newCell (Just p) <*> newIORef 1

wakeLoop : Loop -> IO ()
wakeLoop loop = do
  mutexAcquire loop.wakeHandle.mutex
  handle <- readIORef loop.wakeHandle.value
  case handle of
    Nothing => pure ()
    Just p => ignore (primIO (primWake p))
  mutexRelease loop.wakeHandle.mutex

closeLoop : Loop -> IO ()
closeLoop loop = do
  mutexAcquire loop.wakeHandle.mutex
  writeIORef loop.wakeHandle.value Nothing
  primIO (primFree loop.poller)
  mutexRelease loop.wakeHandle.mutex

waitBlocking : List (Cell Bool) -> IO Step -> IO Step
waitBlocking [] next = next
waitBlocking (done :: rest) next = do
  complete <- readCell done
  if complete
    then pure (Ready (waitBlocking rest next))
    else do
      t <- nowMs
      pure (Sleeping (t + 1) False (waitBlocking (done :: rest) next))

pruneJobs : List (Cell Bool) -> IO (List (Cell Bool))
pruneJobs [] = pure []
pruneJobs (c :: cs) = do
  complete <- readCell c
  rest <- pruneJobs cs
  pure (if complete then rest else c :: rest)

pruneChildren : List Control -> IO (List Control)
pruneChildren [] = pure []
pruneChildren (c :: cs) = do
  done <- readCell c.complete
  rest <- pruneChildren cs
  pure (if done then rest else c :: rest)

cancelTree : Control -> IO ()
cancelTree c = writeCell c.canceled True >> c.notify

-- Scope close always joins children before publishing the parent's result.
closeChildren : List Control -> IO () -> IO Step
closeChildren [] finish = finish $> Done
closeChildren (c :: cs) finish = do
  cancelTree c
  done <- readCell c.complete
  if done
    then pure (Ready (closeChildren cs finish))
    else do
      t <- nowMs
      pure (Sleeping (t + 1) False (closeChildren (c :: cs) finish))

-- Continue only after a whole ownership scope has closed. Every child is
-- canceled before waiting for any one child (siblings may depend on one another).
afterClosed : Step -> IO Step -> IO Step
afterClosed Done next = pure (Ready next)
afterClosed (Ready step) next = pure (Ready (step >>= \s => afterClosed s next))
afterClosed (Sleeping deadline interruptible step) next =
  pure (Sleeping deadline interruptible (step >>= \s => afterClosed s next))
afterClosed (Waiting token interruptible step) next =
  pure (Waiting token interruptible (\event => step event >>= \s => afterClosed s next))

closeScope : Control -> IO Step -> IO Step
closeScope scope next = do
  children <- readIORef scope.children
  traverse_ cancelTree children
  jobs <- readIORef scope.blockingJobs
  closing <- waitBlocking jobs (closeChildren children (pure ()))
  afterClosed closing next

mutual
  evaluate : Loop -> Control -> Bool -> Task es a -> (Outcome es a -> IO Step) -> IO Step
  evaluate loop ctl masked task done = do
    requested <- readCell ctl.canceled
    if requested && not masked
      then pure (Ready (done Canceled))
      else instruction loop ctl masked task done

  instruction : Loop -> Control -> Bool -> Task es a -> (Outcome es a -> IO Step) -> IO Step
  instruction loop ctl masked (Value v) done = pure (Ready (done (Succeeded v)))
  instruction loop ctl masked (Raise e) done = pure (Ready (done (Error e)))
  instruction loop ctl masked (Bind act next) done =
    pure $ Ready $ evaluate loop ctl masked act $ \out => case out of
      Succeeded v => pure (Ready (evaluate loop ctl masked (next v) done))
      Error e => pure (Ready (done (Error e)))
      Canceled => pure (Ready (done Canceled))
  instruction loop ctl masked (Attempt act) done =
    pure $ Ready $ evaluate loop ctl masked act $ \out => case out of
      Succeeded v => pure (Ready (done (Succeeded (Right v))))
      Error e => pure (Ready (done (Succeeded (Left e))))
      Canceled => pure (Ready (done Canceled))
  instruction loop ctl masked (Sync io) done = do
    v <- io
    pure (Ready (done (Succeeded v)))
  instruction loop ctl masked (Blocking io) done = do
    result <- newCell Nothing
    complete <- newCell False
    admitted <- enqueueWork loop.workers $ do
      requested <- readCell ctl.canceled
      unless (requested && not masked) $ do
        value <- io
        writeCell result (Just value)
      writeCell complete True
      wakeLoop loop
    case admitted of
      Left err => pure (Ready (done (Succeeded (Left err))))
      Right () => do
        live <- pruneJobs !(readIORef ctl.blockingJobs)
        writeIORef ctl.blockingJobs (complete :: live)
        awaitBlocking loop ctl masked result complete done
  instruction loop ctl masked (AwaitFD fd events) done = do
    token <- readIORef loop.nextToken
    if token <= 0 || token == 9223372036854775807
      then pure (Ready (done (Succeeded (Left RegistrationFailed))))
      else do
        writeIORef loop.nextToken (token + 1)
        rc <- primIO (primAdd loop.poller fd events token)
        if rc < 0
          then pure (Ready (done (Succeeded (Left RegistrationFailed))))
          else pure (Waiting token (not masked) (\event => evaluate loop ctl masked (Value event) done))
  instruction loop ctl masked (Pause ms) done = do
    t <- nowMs
    pure (Sleeping (t + cast ms) (not masked) (evaluate loop ctl masked (Value ()) done))
  instruction loop ctl masked Cede done = pure (Ready (evaluate loop ctl masked (Value ()) done))
  instruction loop ctl masked CancelSelf done = pure (Ready (done Canceled))
  instruction loop ctl masked (Interruptible act) done =
    pure (Ready (evaluate loop ctl False act done))
  instruction loop ctl masked (Masked act) done =
    pure (Ready (evaluate loop ctl True act done))
  instruction loop ctl masked (CaptureChildren act) done = do
    scope <- MkControl ctl.canceled ctl.complete <$> newIORef [] <*> newIORef [] <*> pure ctl.notify
    pure $ Ready $ evaluate loop scope masked act $ \out => case out of
      Succeeded value => do
        children <- readIORef scope.children
        existing <- pruneChildren !(readIORef ctl.children)
        writeIORef ctl.children (children ++ existing)
        let handles = map (\child => MkFiber
              (do complete <- readCell child.complete
                  pure (if complete then Just (Succeeded ()) else Nothing))
              (cancelTree child)) children
        pure (Ready (done (Succeeded (value, handles))))
      Error err => closeScope scope (done (Error err))
      Canceled => closeScope scope (done Canceled)
  instruction loop ctl masked (Start act) done = do
    (child, fiber, job) <- makeJob loop act
    live <- pruneChildren !(readIORef ctl.children)
    writeIORef ctl.children (child :: live)
    modifyIORef loop.pending (job ::)
    pure (Ready (done (Succeeded fiber)))
  instruction loop ctl masked (Join fiber) done = do
    result <- fiber.observe
    case result of
      Just out => pure (Ready (done (Succeeded out)))
      Nothing => do
        t <- nowMs
        pure (Sleeping (t + 1) (not masked) (evaluate loop ctl masked (Join fiber) done))
  instruction loop ctl masked (RequestCancel fiber) done = do
    fiber.requestCancel
    pure (Ready (done (Succeeded ())))
  instruction loop ctl masked (Finally act cleanup) done = do
    scope <- MkControl ctl.canceled ctl.complete <$> newIORef [] <*> newIORef [] <*> pure ctl.notify
    pure $ Ready $ evaluate loop scope masked act $ \out => do
      children <- readIORef scope.children
      traverse_ cancelTree children
      jobs <- readIORef scope.blockingJobs
      -- Children and active native work stop using the resource before its
      -- finalizer runs. Completion of a callback is not completion of a scope.
      waitBlocking jobs $ closeChildren children (pure ()) >>= resumeCleanup out
    where
      resumeCleanup : Outcome es a -> Step -> IO Step
      resumeCleanup out Done =
        pure $ Ready $ evaluate loop ctl True cleanup $ \fin => case fin of
          Succeeded () => pure (Ready (done out))
          Canceled => pure (Ready (done Canceled))
          Error e => absurd e
      resumeCleanup out (Ready next) = pure (Ready (next >>= resumeCleanup out))
      resumeCleanup out (Sleeping until interruptible next) =
        pure (Sleeping until interruptible (next >>= resumeCleanup out))
      resumeCleanup out (Waiting token interruptible next) =
        pure (Waiting token interruptible (\event => next event >>= resumeCleanup out))
  instruction loop ctl masked (Bracket acquire release use) done = do
    -- Acquisition and use share one child/resource scope. Starting the scope
    -- at use would release the resource before acquisition's watchers stop.
    scope <- MkControl ctl.canceled ctl.complete <$> newIORef [] <*> newIORef [] <*> pure ctl.notify
    pure $ Ready $ evaluate loop scope True acquire $ \out => case out of
      Succeeded resource =>
        pure $ Ready $ evaluate loop scope masked (use resource) $ \used =>
          closeScope scope $ evaluate loop ctl True (release resource) $ \released => case released of
            Succeeded () => pure (Ready (done used))
            Canceled => pure (Ready (done Canceled))
            Error e => absurd e
      Error e => closeScope scope (done (Error e))
      Canceled => closeScope scope (done Canceled)

  awaitBlocking : Loop -> Control -> Bool -> Cell (Maybe a)
               -> Cell Bool -> (Outcome es (Either BlockingError a) -> IO Step) -> IO Step
  awaitBlocking loop ctl masked result complete done = do
    requested <- readCell ctl.canceled
    if requested && not masked
      then waitBlocking [complete] (done Canceled)
      else do
        value <- readCell result
        case value of
          Just v => pure (Ready (done (Succeeded (Right v))))
          Nothing => do
            t <- nowMs
            pure (Sleeping (t + 1) (not masked) (awaitBlocking loop ctl masked result complete done))

  makeJob : Loop -> Task es a -> IO (Control, Fiber es a, Job)
  makeJob loop act = makeJobWithCleanup loop act Nothing

  makeJobWithCleanup : Loop -> Task es a -> Maybe (Task [] ()) -> IO (Control, Fiber es a, Job)
  makeJobWithCleanup loop act cleanup = do
    ctl <- MkControl <$> newCell False <*> newCell False <*> newIORef [] <*> newIORef [] <*> pure (wakeLoop loop)
    result <- newCell Nothing
    let fiber = MkFiber (readCell result) (cancelTree ctl)
    let finish = \out => do
          children <- readIORef ctl.children
          traverse_ cancelTree children
          jobs <- readIORef ctl.blockingJobs
          waitBlocking jobs $ closeChildren children $ do
              writeCell result (Just out)
              writeCell ctl.complete True
    let initial = case cleanup of
          Nothing => evaluate loop ctl False act finish
          Just release => instruction loop ctl False (Finally act release) finish
    pure (ctl, fiber, MkJob ctl (Ready initial))

-- Amortize native poll calls across a bounded instruction slice. No owner
-- monopolizes the loop indefinitely; IO waits and timers end a slice early.
advance : Nat -> Step -> IO Step
advance Z step = pure step
advance (S remaining) (Ready next) = next >>= advance remaining
advance _ step = pure step

stepJob : Loop -> Integer -> Either () (List (Int64, Int)) -> Job -> IO (Maybe Job)
stepJob loop now events job = case job.step of
  Done => pure Nothing
  Ready act => do
    next <- act >>= advance 63
    pure (Just ({ step := next } job))
  Sleeping deadline cancelable act => do
    requested <- readCell job.control.canceled
    if now >= deadline || (cancelable && requested)
      then do
        next <- act
        pure (Just ({ step := next } job))
      else pure (Just job)
  Waiting token cancelable act => do
    requested <- readCell job.control.canceled
    let found = case events of Left () => Just (Left PollFailed); Right pairs => map Right (lookup token pairs)
    let result = if cancelable && requested then Just (Left PollFailed) else found
    case result of
      Nothing => pure (Just job)
      Just event => do
        primIO (primRemove loop.poller token)
        next <- act event
        pure (Just ({ step := next } job))

pollJobs : Loop -> List Job -> IO (Either () (List (Int64, Int)))
pollJobs loop jobs = do
  now <- nowMs
  let timeout = foldl (\delay, job => min delay (waitTime now job.step)) 1000 jobs
  n <- primIO (primWait loop.poller (cast timeout))
  if n < 0 then pure (Left ()) else Right <$> collect 0 n
  where
    waitTime : Integer -> Step -> Integer
    waitTime now (Sleeping deadline _ _) = max 0 (deadline - now)
    waitTime _ (Waiting _ _ _) = 1000
    waitTime _ _ = 0

    collect : Int -> Int -> IO (List (Int64, Int))
    collect i n = if i >= n then pure [] else do
      token <- primIO (primToken loop.poller i)
      events <- primIO (primEvents loop.poller i)
      rest <- collect (i + 1) n
      pure ((token, events) :: rest)

hasReady : Job -> Bool
hasReady job = case job.step of
  Ready _ => True
  Done => True
  _ => False

-- Drive the calling-thread task tree until all owned work has finished.
runLoop : Loop -> List Job -> IO ()
runLoop loop jobs = do
  pending <- readIORef loop.pending
  writeIORef loop.pending []
  let activeJobs = jobs ++ reverse pending
  events <- pollJobs loop activeJobs
  now <- nowMs
  next <- traverse (stepJob loop now events) activeJobs
  let live = mapMaybe id next
  pending <- readIORef loop.pending
  if null live && null pending
    then pure ()
    else do
      runLoop loop live

||| Run a scoped task tree on the calling thread with bounded IO workers.
||| Native startup failure is returned without terminating the host process.
export
runTask : Task es a -> IO (Either String (Outcome es a))
runTask act = do
  pool <- newWorkers
  Right loop <- newLoop pool | Left err => do
    stopWorkers pool
    awaitWorkers pool
    pure (Left err)
  (_, fiber, job) <- makeJob loop act
  runLoop loop [job]
  stopWorkers pool
  awaitWorkers pool
  closeLoop loop
  Just result <- fiber.observe | Nothing => pure (Left "root finished without publishing its outcome")
  pure (Right result)

||| A fixed set of independent owner threads. Roots are distributed round
||| robin; their children never migrate. Constructors remain private.
export
record Runtime where
  constructor MkRuntime
  loops : List Loop
  cursor : Cell Nat
  workers : Workers

public export
data AdmissionError = RuntimeClosed | QueueFull

serviceLoop : Loop -> List Job -> IO ()
serviceLoop loop jobs = do
  (accepting, incoming) <- modifyCell loop.inbox $ \(admitting, queued) =>
    ((admitting, []), (admitting, reverse queued))
  children <- readIORef loop.pending
  writeIORef loop.pending []
  let activeJobs = jobs ++ incoming ++ reverse children
  canceling <- readCell loop.cancelRequested
  when canceling (traverse_ (cancelTree . control) activeJobs)
  events <- pollJobs loop activeJobs
  now <- nowMs
  next <- traverse (stepJob loop now events) activeJobs
  let live = mapMaybe id next
  children <- readIORef loop.pending
  if not accepting && null live && null children
    then closeLoop loop >> writeCell loop.exited True
    else do
      serviceLoop loop live

-- Allocate every native handle before starting owner threads. Unwind all
-- previously allocated handles if a later allocation fails.
allocateLoops : Workers -> Nat -> List Loop -> IO (Either String (List Loop))
allocateLoops pool Z allocated = pure (Right (reverse allocated))
allocateLoops pool (S n) allocated = do
  Right loop <- newLoop pool | Left err => do
    traverse_ closeLoop allocated
    pure (Left err)
  allocateLoops pool n (loop :: allocated)

||| Start independent event loops backed by native readiness polling.
export
startRuntime : (loopCount : Nat) -> IO (Either String Runtime)
startRuntime Z = pure (Left "event-loop count must be positive")
startRuntime n = do
  pool <- newWorkers
  Right loops <- allocateLoops pool n [] | Left err => do
    stopWorkers pool
    awaitWorkers pool
    pure (Left err)
  cursor <- newCell 0
  traverse_ (\loop => ignore (fork (serviceLoop loop []))) loops
  pure (Right (MkRuntime loops cursor pool))

choose : Nat -> List a -> Maybe a
choose _ [] = Nothing
choose Z (a :: _) = Just a
choose (S n) (_ :: as) = choose n as

||| Admit a root to its assigned owner. The mailbox has a fixed capacity of
||| 128; socket/connection admission will impose its own active-work bound.
submitInternal : Runtime -> Task es a -> Maybe (Task [] ()) -> IO (Either AdmissionError (Fiber es a))
submitInternal runtime task cleanup = do
  index <- modifyCell runtime.cursor $ \n =>
    (if S n >= length runtime.loops then 0 else S n, n)
  Just loop <- pure (choose index runtime.loops) | Nothing => pure (Left RuntimeClosed)
  (_, fiber, job) <- makeJobWithCleanup loop task cleanup
  admitted <- modifyCell {b = Either AdmissionError ()} loop.inbox $ \(admitting, queued) =>
    if not admitting then ((admitting, queued), Left RuntimeClosed)
    else if length queued >= 128 then ((admitting, queued), Left QueueFull)
    else ((admitting, job :: queued), Right ())
  wakeLoop loop
  pure (map (const fiber) admitted)

export
submit : Runtime -> Task es a -> IO (Either AdmissionError (Fiber es a))
submit runtime task = submitInternal runtime task Nothing

||| Transfer an already acquired resource with a finalizer installed before
||| cancellation can be observed. On rejected admission the caller still owns
||| the resource and must clean it up. On success only the owner runs cleanup.
export
submitOwned : Runtime -> Task es a -> Task [] () -> IO (Either AdmissionError (Fiber es a))
submitOwned runtime task cleanup = submitInternal runtime task (Just cleanup)

||| Stop admission while allowing existing roots to finish. Call
||| requestShutdown after the application's drain deadline to cancel stragglers.
export
requestDrain : Runtime -> IO ()
requestDrain runtime = traverse_ stop runtime.loops
  where
    stop : Loop -> IO ()
    stop loop = do
      modifyCell loop.inbox (\(_, jobs) => ((False, jobs), ()))
      wakeLoop loop

||| Stop admission and request cancellation on each owner loop. This is
||| nonblocking and idempotent; completion must be observed separately.
export
requestShutdown : Runtime -> IO ()
requestShutdown runtime = traverse_ stop runtime.loops
  where
    stop : Loop -> IO ()
    stop loop = do
      writeCell loop.cancelRequested True
      modifyCell loop.inbox (\(_, jobs) => ((False, jobs), ()))
      wakeLoop loop

||| Observe owner-thread termination without claiming a hard bound on user IO.
export
hasStopped : Runtime -> IO Bool
hasStopped runtime = do
  loopsStopped <- map (all id) (traverse (readCell . exited) runtime.loops)
  if not loopsStopped then pure False else do
    stopWorkers runtime.workers
    map (all id) (traverse readCell runtime.workers.exited)
