module Async.Standalone

%default total

%foreign "C:flux_shutdown_start,libflux_async"
primStart : Int -> PrimIO Int
%foreign "C:flux_shutdown_requested,libflux_async"
primRequested : PrimIO Int
%foreign "C__collect_safe:flux_shutdown_stop,libflux_async"
primStop : PrimIO ()

||| Opt in to process-wide SIGINT/SIGTERM handling and an independent native
||| watchdog. Only standalone runners should use this, never embedded servers.
export
startShutdownWatchdog : Int -> IO (Either String ())
startShutdownWatchdog milliseconds = do
  result <- primIO (primStart milliseconds)
  pure (if result == 0 then Right () else Left ("shutdown watchdog startup failed: " ++ show result))

export
shutdownRequested : IO Bool
shutdownRequested = map (/= 0) (primIO primRequested)

export
stopShutdownWatchdog : IO ()
stopShutdownWatchdog = primIO primStop
