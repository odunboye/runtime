module Stream.Posix

import public Stream
import public System.Posix.Dir
import public System.Posix.File.Stats
import public System.Posix.File.Type
import public System.Posix.File
import System.Posix.File.Prim as F

%default covering

||| Regular-file reads run on the bounded worker pool. Cancellation retains
||| ownership until an in-flight read returns; it never abandons a reader.
export
bytes : FileDesc a => Has Errno es => a -> Bits32 -> Stream Task es ByteString
bytes fd size = P.unfoldEvalMaybe $ do
  if size == 0 then throw EINVAL else pure ()
  Right result <- blocking (the (IO (Result [Errno] ByteString)) (runIO (\t => toResult (F.read fd ByteString (min size 65536) t))))
    | Left _ => throw EBUSY
  case result of
    Left (Here err) => throw err
    Right chunk => pure (ByteString.nonEmpty chunk)

export
readBytes : Has Errno es => {default 65536 size : Bits32} -> String -> Stream Task es ByteString
readBytes path = resource (openFile path O_RDONLY 0) (\fd => bytes fd size)
