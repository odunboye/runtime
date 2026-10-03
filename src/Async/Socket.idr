module Async.Socket

import public Async.Core
import Data.Buffer
import Data.IORef
import Data.List
import System.Concurrency

%default covering

%foreign "C:flux_socket_listen,libflux_async"
primListen : String -> Int -> Int -> PrimIO Int
%foreign "C:flux_socket_accept,libflux_async"
primAccept : Int -> PrimIO Int
%foreign "C:flux_socket_port,libflux_async"
primPort : Int -> PrimIO Int
%foreign "C:flux_socket_recv,libflux_async"
primRecv : Int -> Buffer -> Int -> PrimIO Int
%foreign "C:flux_socket_send,libflux_async"
primSend : Int -> Buffer -> Int -> Int -> PrimIO Int
%foreign "C:flux_socket_close,libflux_async"
primClose : Int -> PrimIO Int
%foreign "C:flux_socket_would_block,libflux_async"
wouldBlock : Int -> Int

public export
data SocketError = SocketIO Int | SocketClosed | WrongOwner | ReadinessFailure | InvalidBufferSize | SocketBusy

||| The descriptor is hidden. First use claims its owner thread; accepted
||| sockets can be passed to another loop before that first use.
export
record Socket where
  constructor MkSocket
  fd : Int
  lock : Mutex
  owner : IORef (Maybe Int)
  closed : IORef Bool
  reading : IORef Bool
  writing : IORef Bool

newSocket : Int -> IO Socket
newSocket fd = MkSocket fd <$> makeMutex <*> newIORef Nothing <*> newIORef False <*> newIORef False <*> newIORef False

claim : Socket -> IO (Either SocketError Int)
claim socket = do
  thread <- getThreadId
  mutexAcquire socket.lock
  closed <- readIORef socket.closed
  owner <- readIORef socket.owner
  result <- if closed then pure (Left SocketClosed) else case owner of
    Nothing => writeIORef socket.owner (Just thread) $> Right socket.fd
    Just n => pure (if n == thread then Right socket.fd else Left WrongOwner)
  mutexRelease socket.lock
  pure result

-- One operation per direction: sibling tasks may read and write concurrently,
-- but two writes cannot interleave chunks and two readers cannot consume one
-- another's protocol bytes. Busy operations fail instead of blocking owners.
withOperation : Socket -> IORef Bool -> (Int -> Task es (Either SocketError a))
             -> Task es (Either SocketError a)
withOperation socket busy action = bracket
  (liftIO $ the (IO (Either SocketError Int)) $ do
    Right fd <- claim socket | Left err => pure (Left err)
    mutexAcquire socket.lock
    active <- readIORef busy
    unless active (writeIORef busy True)
    mutexRelease socket.lock
    pure (if active then Left SocketBusy else Right fd))
  (\result => case result of
    Left _ => pure ()
    Right _ => liftIO $ do
      mutexAcquire socket.lock
      writeIORef busy False
      mutexRelease socket.lock)
  (\result => case result of Left err => pure (Left err); Right fd => action fd)

||| Creates an unclaimed listener; port zero asks the OS for an available port.
export
listen : String -> Int -> Int -> IO (Either SocketError Socket)
listen host port backlog = do
  fd <- primIO (primListen host port backlog)
  if fd < 0 then pure (Left (SocketIO fd)) else Right <$> newSocket fd

||| Metadata lookup does not claim an owner or perform asynchronous IO.
export
localPort : Socket -> IO (Either SocketError Int)
localPort socket = do
  mutexAcquire socket.lock
  closed <- readIORef socket.closed
  n <- the (IO Int) (if closed then pure (-1) else primIO (primPort socket.fd))
  mutexRelease socket.lock
  pure (if closed then Left SocketClosed else if n < 0 then Left (SocketIO n) else Right n)

||| Idempotent, owner-checked close. Use as a bracket finalizer after children
||| have joined; calling it while sibling tasks use the socket is unsupported.
export
close : Socket -> Task es (Either SocketError ())
close socket = liftIO $ do
  thread <- getThreadId
  mutexAcquire socket.lock
  closed <- readIORef socket.closed
  owner <- readIORef socket.owner
  result <- if closed then pure (Right ()) else case owner of
    Just n => if n /= thread then pure (Left WrongOwner) else dispose
    Nothing => dispose
  mutexRelease socket.lock
  pure result
  where
    dispose : IO (Either SocketError ())
    dispose = do
      writeIORef socket.closed True
      rc <- primIO (primClose socket.fd)
      pure (if rc < 0 then Left (SocketIO rc) else Right ())

acceptFD : Int -> Task es (Either SocketError Socket)
acceptFD fd = do
  n <- liftIO (primIO (primAccept fd))
  if n >= 0
    then map Right (liftIO (newSocket n))
    else if wouldBlock n == 0 then pure (Left (SocketIO n))
    else do
      Right _ <- Interruptible (awaitFD fd 1) | Left _ => pure (Left ReadinessFailure)
      acceptFD fd

||| Acquire with bracket (or mask an ownership transfer) so a newly accepted
||| descriptor is registered for cleanup before cancellation is observed.
export
accept : Socket -> Task es (Either SocketError Socket)
accept listener = withOperation listener listener.reading acceptFD

readBuffer : Buffer -> Int -> Int -> IO (List Bits8)
readBuffer buffer i n = if i >= n then pure [] else do
  byte <- getBits8 buffer i
  rest <- readBuffer buffer (i + 1) n
  pure (byte :: rest)

||| Read up to 64 KiB. Empty output means EOF, never "try again".
receiveFD : Int -> Int -> Task es (Either SocketError (List Bits8))
receiveFD fd size =
  if size < 1 || size > 65536 then pure (Left InvalidBufferSize) else do
    Just buffer <- liftIO (newBuffer size) | Nothing => pure (Left InvalidBufferSize)
    n <- liftIO (primIO (primRecv fd buffer size))
    if n >= 0
      then map Right (liftIO (readBuffer buffer 0 n))
      else if wouldBlock n == 0 then pure (Left (SocketIO n))
      else do
        Right _ <- awaitFD fd 1 | Left _ => pure (Left ReadinessFailure)
        receiveFD fd size

export
receive : Socket -> Int -> Task es (Either SocketError (List Bits8))
receive socket size = withOperation socket socket.reading (\fd => receiveFD fd size)

fillBuffer : Buffer -> Int -> List Bits8 -> IO ()
fillBuffer _ _ [] = pure ()
fillBuffer buffer i (b :: bs) = setBits8 buffer i b >> fillBuffer buffer (i + 1) bs

sendBuffer : Int -> Buffer -> Int -> Int -> Task es (Either SocketError ())
sendBuffer fd buffer offset size =
  if offset >= size then pure (Right ()) else do
    n <- liftIO (primIO (primSend fd buffer offset (size - offset)))
    if n > 0
      then yield >> sendBuffer fd buffer (offset + n) size
      else if n == 0 then pure (Left SocketClosed)
      else if wouldBlock n == 0 then pure (Left (SocketIO n))
      else do
        Right _ <- awaitFD fd 2 | Left _ => pure (Left ReadinessFailure)
        sendBuffer fd buffer offset size

||| Writes all bytes with backpressure. Buffers are capped at 64 KiB.
sendFD : Int -> List Bits8 -> Task es (Either SocketError ())
sendFD fd [] = pure (Right ())
sendFD fd bytes = do
  let (chunk, rest) = splitAt 65536 bytes
  let size = cast (length chunk)
  Just buffer <- liftIO (newBuffer size) | Nothing => pure (Left InvalidBufferSize)
  liftIO (fillBuffer buffer 0 chunk)
  Right () <- sendBuffer fd buffer 0 size | Left err => pure (Left err)
  sendFD fd rest

export
send : Socket -> List Bits8 -> Task es (Either SocketError ())
send socket bytes = withOperation socket socket.writing (\fd => sendFD fd bytes)
