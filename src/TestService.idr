module TestService

import Async.Core
import Async.Runner
import TestSupport
import Data.IORef
import Data.List
import System
import System.Clock
import System.Concurrency

%default covering

nowMs : IO Integer
nowMs = do
  t <- clockTime Monotonic
  pure (seconds t * 1000 + nanoseconds t `div` 1000000)

awaitUntil : Integer -> IO (Maybe a) -> IO (Maybe a)
awaitUntil deadline action = do
  result <- action
  case result of
    Just a => pure (Just a)
    Nothing => do
      now <- nowMs
      if now >= deadline
        then pure Nothing
        else usleep 1000 >> awaitUntil deadline action

check : IORef Nat -> String -> Bool -> IO ()
check failures label ok = do
  putStrLn ((if ok then "PASS " else "FAIL ") ++ label)
  unless ok (modifyIORef failures S)

affinity : Task [] (Int, Int, Int)
affinity = do
  before <- liftIO getThreadId
  child <- spawn (the (Task [] Int) (liftIO getThreadId))
  childResult <- join child
  sleep 2
  after <- liftIO getThreadId
  pure (before, after, case childResult of Succeeded n => n; _ => -1)

-- The writer is another task on this same event loop. Use a handshake instead
-- of assuming that a wall-clock sleep guarantees a particular interleaving.
awaitStarted : IORef Bool -> Task [] ()
awaitStarted ref = do
  started <- liftIO (readIORef ref)
  if started then pure () else yield >> awaitStarted ref

main : IO ()
main = do
  failures <- newIORef 0
  invalid <- startRuntime 0
  check failures "zero event loops rejected" (case invalid of Left _ => True; _ => False)
  Right runtime <- startRuntime 2 | Left error => putStrLn error >> exitFailure
  Right fa <- submit runtime affinity | Left _ => exitFailure
  Right fb <- submit runtime affinity | Left _ => exitFailure
  deadline <- map (+ 5000) nowMs
  a <- awaitUntil deadline fa.observe
  b <- awaitUntil deadline fb.observe
  check failures "roots run on distinct event-loop threads" $ case (a, b) of
    (Just (Succeeded (x, _, _)), Just (Succeeded (y, _, _))) => x /= y
    _ => False
  check failures "continuations and children retain their owner" $ case (a, b) of
    (Just (Succeeded (x, y, z)), Just (Succeeded (u, v, w))) => x == y && y == z && u == v && v == w
    _ => False

  Right work <- submit runtime (the (Task [] (Int, Either BlockingError Int, Int)) $ do
    owner <- liftIO getThreadId
    worker <- blocking (usleep 30000 >> getThreadId)
    resumed <- liftIO getThreadId
    pure (owner, worker, resumed)) | Left _ => exitFailure
  outcome <- awaitUntil deadline work.observe
  check failures "blocking work offloads and resumes on original owner" $ case outcome of
    Just (Succeeded (owner, Right worker, resumed)) => owner /= worker && owner == resumed
    _ => False

  -- Channel handshakes establish that the worker is running before cancel.
  -- Its completion and finalizer markers are only read after joined completion.
  started <- makeChannel
  workerDone <- newIORef False
  releasedAfterDone <- newIORef False
  Right busy <- submit runtime (the (Task [] ()) $
    guarantee
      (ignore $ blocking $ do
        channelPut started ()
        usleep 50000
        writeIORef workerDone True)
      (liftIO $ readIORef workerDone >>= writeIORef releasedAfterDone)) | Left _ => exitFailure
  didStart <- awaitUntil deadline (channelGetNonBlocking started)
  check failures "blocking worker starts" (case didStart of Just () => True; _ => False)
  busy.requestCancel
  canceled <- awaitUntil deadline busy.observe
  released <- readIORef releasedAfterDone
  check failures "cancel waits for worker before resource finalizer" $
    released && case canceled of Just Canceled => True; _ => False

  -- Acquisition may itself start a resource-owning child (for example a
  -- connection watcher). It belongs to the bracket, not to the outer root.
  acquireChildStarted <- newIORef False
  acquireChildStopped <- newIORef False
  bracketReleasedSafely <- newIORef False
  Right acquisition <- submit runtime (the (Task [] ()) $ bracket
    (do
      _ <- spawn (the (Task [] ()) $ guarantee
        (liftIO (writeIORef acquireChildStarted True) >> sleep 10000)
        (liftIO (writeIORef acquireChildStopped True)))
      awaitStarted acquireChildStarted)
    (\_ => liftIO $ readIORef acquireChildStopped >>= writeIORef bracketReleasedSafely)
    (\_ => pure ())) | Left _ => exitFailure
  acquisitionResult <- awaitUntil deadline acquisition.observe
  childStarted <- readIORef acquireChildStarted
  releaseSafe <- readIORef bracketReleasedSafely
  check failures "bracket joins acquisition children before release" $
    childStarted && releaseSafe && case acquisitionResult of Just (Succeeded ()) => True; _ => False

  requestShutdown runtime
  stopped <- awaitUntil deadline $ do
    yes <- hasStopped runtime
    pure (if yes then Just () else Nothing)
  check failures "all event loops and workers stop" (case stopped of Just () => True; _ => False)
  rejected <- submit runtime (the (Task [] ()) (pure ()))
  check failures "submission after shutdown rejected" (case rejected of Left RuntimeClosed => True; _ => False)
  Right draining <- startRuntime 1 | Left _ => exitFailure
  Right finishing <- submit draining (the (Task [] Nat) (sleep 25 >> pure 19)) | Left _ => exitFailure
  requestDrain draining
  rejected <- submit draining (the (Task [] ()) (pure ()))
  check failures "drain closes admission" (case rejected of Left RuntimeClosed => True; _ => False)
  completed <- awaitUntil deadline finishing.observe
  check failures "drain preserves in-flight work" (case completed of Just (Succeeded 19) => True; _ => False)
  stopped <- awaitUntil deadline $ do
    yes <- hasStopped draining
    pure (if yes then Just () else Nothing)
  check failures "drained runtime stops its owners and workers" (case stopped of Just () => True; _ => False)

  Right draining <- startRuntime 1 | Left _ => exitFailure
  Right stuck <- submit draining (the (Task [] ()) (sleep 10000)) | Left _ => exitFailure
  requestDrain draining
  requestShutdown draining
  completed <- awaitUntil deadline stuck.observe
  check failures "drain can escalate to cancellation" (case completed of Just Canceled => True; _ => False)
  stopped <- awaitUntil deadline $ do
    yes <- hasStopped draining
    pure (if yes then Just () else Nothing)
  check failures "escalated runtime stops" (case stopped of Just () => True; _ => False)
  Right ownedRuntime <- startRuntime 1 | Left _ => exitFailure
  entered <- makeChannel
  resume <- makeChannel
  Right blocker <- submit ownedRuntime (the (Task [] ()) (liftIO (channelPut entered () >> channelGet resume)))
    | Left _ => exitFailure
  _ <- awaitUntil deadline (channelGetNonBlocking entered)
  released <- newIORef False
  executed <- newIORef False
  Right owned <- submitOwned ownedRuntime (the (Task [] ()) (liftIO (writeIORef executed True)))
    (liftIO (writeIORef released True)) | Left _ => exitFailure
  requestShutdown ownedRuntime
  channelPut resume ()
  completed <- awaitUntil deadline owned.observe
  cleanup <- readIORef released
  body <- readIORef executed
  check failures "pre-start cancellation runs transferred resource cleanup" $
    cleanup && not body && case completed of Just Canceled => True; _ => False
  stopped <- awaitUntil deadline $ do
    yes <- hasStopped ownedRuntime
    pure (if yes then Just () else Nothing)
  check failures "ownership-transfer runtime stops" (case stopped of Just () => True; _ => False)
  n <- readIORef failures
  if n == 0 then putStrLn "All runtime service checks passed" else exitFailure
