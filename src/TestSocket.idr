module TestSocket

import Async.Core
import Async.Runner
import TestSupport
import Async.Socket
import Data.IORef
import System
import System.File

%default covering

echo : Socket -> Task [] ()
echo socket = do
  Right bytes <- receive socket 4096 | Left _ => liftIO exitFailure
  case bytes of
    [] => pure ()
    _ => do
      Right () <- send socket bytes | Left _ => liftIO exitFailure
      echo socket

serve : Nat -> Socket -> Task [] ()
serve Z _ = pure ()
serve (S n) listener = do
  bracket (accept listener)
    (\result => case result of Right socket => ignore (close socket); Left _ => pure ())
    (\result => case result of Right socket => echo socket; Left _ => liftIO exitFailure)
  serve n listener

main : IO ()
main = do
  Right listener <- listen "127.0.0.1" 0 16 | Left _ => exitFailure
  Right port <- localPort listener | Left _ => exitFailure
  putStrLn (show port)
  fflush stdout
  Succeeded () <- runTestTask (guarantee (serve 4 listener) (ignore (close listener)))
    | _ => exitFailure
  -- Cancellation of accept must interrupt the readiness wait even though
  -- bracket masks acquisition while publishing an accepted descriptor.
  Right idle <- listen "127.0.0.1" 0 16 | Left _ => exitFailure
  Succeeded (Left ()) <- runTestTask (the (Task [] (Either () ())) $
    race (sleep 5)
      (guarantee (serve 1 idle) (ignore (close idle)))) | _ => exitFailure
  putStrLn "PASS socket echo, EOF, accept cancellation"
