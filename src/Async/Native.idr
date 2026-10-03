module Async.Native

import System.FFI

%default total

-- Internal owner-thread API. Wake is the only cross-thread operation.
-- The runner must stop producers before freeing the handle.
%foreign "C:flux_poller_new,libflux_async"
export primNew : PrimIO AnyPtr

%foreign "C:flux_poller_free,libflux_async"
export primFree : AnyPtr -> PrimIO ()

%foreign "C:flux_poller_wake,libflux_async"
export primWake : AnyPtr -> PrimIO Int

%foreign "C:flux_poller_add,libflux_async"
export primAdd : AnyPtr -> Int -> Int -> Int64 -> PrimIO Int

%foreign "C:flux_poller_remove,libflux_async"
export primRemove : AnyPtr -> Int64 -> PrimIO ()

%foreign "C__collect_safe:flux_poller_wait,libflux_async"
export primWait : AnyPtr -> Int -> PrimIO Int

%foreign "C:flux_poller_token,libflux_async"
export primToken : AnyPtr -> Int -> PrimIO Int64

%foreign "C:flux_poller_events,libflux_async"
export primEvents : AnyPtr -> Int -> PrimIO Int

%foreign "C:flux_monotonic_ms,libflux_async"
export primNow : PrimIO Int64
