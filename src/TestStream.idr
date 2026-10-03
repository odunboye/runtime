module TestStream

import Async.Runner
import TestSupport
import Stream.Resource as R
import Stream.Pull as P
import Data.IORef
import System

%default covering

check : IORef Nat -> String -> Bool -> IO ()
check failures name good = do
  putStrLn ((if good then "PASS " else "FAIL ") ++ name)
  unless good (modifyIORef failures S)

tracked : IORef (List String) -> String -> Stream Task [String] Nat
tracked log label = R.bracket
  (liftIO (modifyIORef log (++ ["acquire " ++ label])))
  (\_ => liftIO (modifyIORef log (++ ["release " ++ label])))
  (\_ => P.emits [1,2,3])

main : IO ()
main = do
  failures <- newIORef 0
  result <- runTestTask $ pullIn $ the (Pull Task Void [] Nat) $
    P.foldGet (+) 0 (P.emits [1,2,3,4])
  check failures "stream fold preserves values" (case result of Succeeded 10 => True; _ => False)

  log <- newIORef []
  result <- runTestTask $ pullIn $ P.drain (tracked log "normal")
  entries <- readIORef log
  check failures "normal completion releases once" (entries == ["acquire normal", "release normal"])

  writeIORef log []
  result <- runTestTask $ pullIn $ the (Pull Task Void [String] ()) $ do
    P.drain (P.take 1 (tracked log "early"))
    exec (liftIO (modifyIORef log (++ ["after"])))
  entries <- readIORef log
  check failures "early termination releases before continuation" (entries == ["acquire early", "release early", "after"])

  writeIORef log []
  result <- runTestTask $ pullIn $ the (Pull Task Void [String] ()) $ do
    _ <- attempt $ the (Pull Task Void [String] ()) $ R.bracket
      (liftIO (modifyIORef log (++ ["acquire"])))
      (\_ => liftIO (modifyIORef log (++ ["release"])))
      (\_ => throw "expected")
    exec (liftIO (modifyIORef log (++ ["caught"])))
  entries <- readIORef log
  check failures "caught failure releases before handler continues" (entries == ["acquire", "release", "caught"])

  writeIORef log []
  result <- runTestTask $ pullIn $ the (Pull Task Void [] ()) $
    R.bracket (pure ()) (\_ => liftIO (modifyIORef log (++ ["outer"]))) $ \_ =>
      R.bracket (pure ()) (\_ => liftIO (modifyIORef log (++ ["inner"]))) $ \_ => pure ()
  entries <- readIORef log
  check failures "nested finalizers run inside out" (entries == ["inner", "outer"])

  count <- newIORef 0
  result <- runTestTask $ the (Task [] ()) $ do
    child <- spawn $ pullIn $ the (Pull Task Void [] ()) $
      R.bracket (pure ()) (\_ => liftIO (modifyIORef count S)) $ \_ => exec (sleep 10000)
    sleep 5
    cancel child
  n <- readIORef count
  check failures "cancellation finalizes suspended stream once" (n == 1)
  childStopped <- newIORef False
  releasedSafely <- newIORef False
  result <- runTestTask $ pullIn $ the (Pull Task Void [] ()) $ P.drain $ P.take 1 $
    R.bracket (pure ())
      (\_ => liftIO (readIORef childStopped >>= writeIORef releasedSafely)) $ \_ => do
        _ <- exec $ spawn $ the (Task [] ()) $ guarantee
          (sleep 10000) (liftIO (writeIORef childStopped True))
        exec (sleep 5)
        P.emits [1,2,3]
  safe <- readIORef releasedSafely
  check failures "early stream close joins spawned children before resource release" safe

  n <- readIORef failures
  if n == 0 then putStrLn "All stream checks passed" else exitFailure
