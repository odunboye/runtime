module TestMain

import Async.Core
import Async.Runner
import TestSupport
import Data.IORef
import System

%default covering

check : IORef Nat -> String -> Bool -> IO ()
check failures name good = do
  putStrLn ((if good then "PASS " else "FAIL ") ++ name)
  unless good (modifyIORef failures S)

deep : Nat -> Task [] Nat
deep Z = pure 0
deep (S k) = map S (deep k)

main : IO ()
main = do
  failures <- newIORef 0
  value <- runTestTask (the (Task [] Nat) (pure 42))
  check failures "pure result" (case value of Succeeded 42 => True; _ => False)
  value <- runTestTask (deep 20000)
  check failures "deep bind without interpreter stack recursion" (case value of Succeeded n => n == 20000; _ => False)
  value <- runTestTask (the (Task [] (Result [String] Nat)) (attempt (throw "expected")))
  check failures "typed failure caught across error lists" (case value of Succeeded (Left _) => True; _ => False)

  count <- newIORef 0
  value <- runTestTask $ the (Task [] ()) $ do
    child <- spawn (the (Task [] ()) (guarantee (sleep 10000) (liftIO (modifyIORef count S))))
    sleep 2
    cancel child
    cancel child
  n <- readIORef count
  check failures "cancel joins and cleanup runs once" (n == 1)

  count <- newIORef 0
  value <- runTestTask $ the (Task [] ()) $ do
    _ <- spawn (the (Task [] ()) (guarantee (sleep 10000) (liftIO (modifyIORef count S))))
    sleep 2
  n <- readIORef count
  check failures "scope exit cancels and joins child" (n == 1)

  acquired <- newIORef False
  released <- newIORef False
  used <- newIORef False
  value <- runTestTask $ the (Task [] ()) $ do
    child <- spawn $ the (Task [] ()) $ bracket
      (liftIO (writeIORef acquired True) >> sleep 10 >> pure ())
      (\_ => liftIO (writeIORef released True))
      (\_ => liftIO (writeIORef used True))
    sleep 2
    cancel child
  a <- readIORef acquired
  r <- readIORef released
  u <- readIORef used
  check failures "cancel during acquire releases resource without use" (a && r && not u)

  count <- newIORef 0
  value <- runTestTask $ the (Task [] (Either Nat String)) $
    race (sleep 2 >> pure 7)
         (guarantee (sleep 10000 >> pure "late") (liftIO (modifyIORef count S)))
  n <- readIORef count
  check failures "race waits for loser finalization" (n == 1 && case value of Succeeded (Left 7) => True; _ => False)

  count <- newIORef 0
  value <- runTestTask $ the (Task [String] ()) $
    guarantee (throw "failed") (liftIO (modifyIORef count S))
  n <- readIORef count
  check failures "cleanup on typed failure" (n == 1 && case value of Error _ => True; _ => False)
  n <- readIORef failures
  if n == 0 then putStrLn "All runtime checks passed" else exitFailure
