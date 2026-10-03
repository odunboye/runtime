module Async.Server

import Async.Core
import Async.Runner
import Async.Socket as S
import Data.IORef
import System.Clock
import System.Concurrency

%default covering

record Admission where
  constructor MkAdmission
  mutex : Mutex
  count : IORef Nat

reserve : Admission -> Nat -> IO Bool
reserve admission limit = do
  mutexAcquire admission.mutex
  n <- readIORef admission.count
  let available = n < limit
  when available (writeIORef admission.count (S n))
  mutexRelease admission.mutex
  pure available

release : Admission -> IO ()
release admission = do
  mutexAcquire admission.mutex
  modifyIORef admission.count (\n => n `minus` 1)
  mutexRelease admission.mutex

nowMs : IO Integer
nowMs = do
  t <- clockTime Monotonic
  pure (seconds t * 1000 + nanoseconds t `div` 1000000)

awaitStop : Runtime -> Task es ()
awaitStop runtime = do
  stopped <- liftIO (hasStopped runtime)
  unless stopped (sleep 1 >> awaitStop runtime)

drainUntil : Runtime -> Integer -> Task es ()
drainUntil runtime deadline = do
  stopped <- liftIO (hasStopped runtime)
  unless stopped $ do
    now <- liftIO nowMs
    if now >= deadline
      then liftIO (requestShutdown runtime) >> awaitStop runtime
      else sleep 1 >> drainUntil runtime deadline

-- Reservation and transfer are masked. Once submitted, only the destination
-- owner touches the accepted socket, including its finalizer. Rejected work
-- still belongs to this loop and is closed here.
acceptLoop : Runtime -> Admission -> Nat -> Socket -> (Socket -> Task [] ()) -> Task [String] ()
acceptLoop runtime admission limit listener handler = do
  accepted <- Masked $ do
    reserved <- liftIO (reserve admission limit)
    if not reserved then pure False else do
      -- Bracket the reservation even when cancellation interrupts accept.
      handedOff <- liftIO (newIORef False)
      guarantee
        (do
          result <- S.accept listener
          case result of
            Left _ => throw "listener accept failed"
            Right peer => do
              result <- liftIO $ submitOwned runtime (handler peer)
                (ignore (S.close peer) >> liftIO (release admission))
              case result of
                Right _ => liftIO (writeIORef handedOff True) $> True
                Left _ => ignore (S.close peer) $> False)
        (do
          transferred <- liftIO (readIORef handedOff)
          unless transferred (liftIO (release admission)))
  unless accepted (sleep 1)
  acceptLoop runtime admission limit listener handler

||| Serve a borrowed listener with bounded active connections. Accepted peers
||| belong to fixed round-robin owner loops. A stop request ends admission,
||| allows the configured drain interval, then cancels and joins stragglers.
||| This library function never exits the process or abandons blocked work.
export
serve : (owners : Nat) -> (maxConnections : Nat) -> (drainMs : Nat)
     -> Socket -> (Socket -> Task [] ()) -> Task [] () -> Task [String] ()
serve owners Z drainMs listener handler stop = throw "maximum connections must be positive"
serve owners limit drainMs listener handler stop = bracket
  (do
    Right runtime <- liftIO (startRuntime owners) | Left err => throw err
    pure runtime)
  (\runtime => liftIO (requestShutdown runtime) >> awaitStop runtime)
  (\runtime => do
    admission <- liftIO (MkAdmission <$> makeMutex <*> newIORef 0)
    ignore $ race (acceptLoop runtime admission limit listener handler) (weakenErrors stop)
    liftIO (requestDrain runtime)
    deadline <- map (+ cast drainMs) (liftIO nowMs)
    drainUntil runtime deadline)
