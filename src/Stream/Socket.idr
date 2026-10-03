module Stream.Socket

import public Stream
import public Async.Socket
import public System.Posix.Errno
import Data.ByteString

%default covering

export
socketResult : Has Errno es => Either SocketError a -> Task es a
socketResult (Right value) = pure value
socketResult (Left (SocketIO code)) = throw (EN (cast (negate code)))
socketResult (Left _) = throw EIO

||| The stream borrows its socket. Its enclosing task must bracket ownership
||| so cancellation joins users before closing the descriptor.
export
bytes : Has Errno es => Socket -> Bits32 -> Stream Task es ByteString
bytes socket size = P.unfoldEvalMaybe $ do
  chunk <- receive socket (cast (min size 65536)) >>= socketResult
  pure (case chunk of [] => Nothing; _ => Just (pack chunk))

export
writeTo : Has Errno es => Socket -> Pull Task ByteString es r -> Pull Task Void es r
writeTo socket = P.foreach (\chunk => send socket (unpack chunk) >>= socketResult)
