module Stream.Core

import public Async.Core
import Data.IORef
import Data.List

%default covering

record Hook (f : List Type -> Type -> Type) where
  constructor MkHook
  done : IORef Bool
  action : f [] ()

record Scope (f : List Type -> Type -> Type) where
  constructor MkScope
  hooks : IORef (List (Hook f))
  closed : IORef Bool
  children : IORef (List (Fiber [] ()))
  ancestors : List (IORef (List (Fiber [] ())))

public export
data Pull : (List Type -> Type -> Type) -> Type -> List Type -> Type -> Type where
  Val : r -> Pull f o es r
  Err : HSum es -> Pull f o es r
  Cons : o -> Inf (Pull f o es r) -> Pull f o es r
  Exec : f es r -> Pull f o es r
  Bind : Pull f o es a -> (a -> Pull f o es r) -> Pull f o es r
  Att : Pull f o es r -> Pull f o fs (Result es r)
  Uncons : Pull f o es r -> Pull f q es (Either r (o, Pull f o es r))
  Scoped : Pull f o es r -> Pull f o es r
  InScope : Scope f -> Pull f o es r -> Pull f o es r
  Acquire : f es r -> (r -> f [] ()) -> Pull f o es r

public export
0 Stream : (List Type -> Type -> Type) -> List Type -> Type -> Type
Stream f es o = Pull f o es ()

public export
MErr (Pull f o) where
  fail = Err
  succeed = Val
  bind = Bind
  attempt = Att
  mapImpl f p = Bind p (Val . f)
  appImpl f p = Bind f (\g => Bind p (Val . g))

export
ELift1 s f => ELift1 s (Pull f o) where
  elift1 = Exec . elift1

export
HasIO (f es) => HasIO (Pull f o es) where
  liftIO = Exec . liftIO

export
Semigroup (Stream f es o) where
  a <+> b = a >> b

export
Monoid (Stream f es o) where
  neutral = pure ()

export
exec : f es a -> Pull f o es a
exec = Exec

export
cons : o -> Inf (Pull f o es r) -> Pull f o es r
cons = Cons

export
emit : o -> Stream f es o
emit v = Cons v (Val ())

export
uncons : Pull f o es r -> Pull f q es (Either r (o, Pull f o es r))
uncons = Uncons

export
newScope : Pull f o es r -> Pull f o es r
newScope = Scoped

export
acquire : f es r -> (r -> f [] ()) -> Pull f o es r
acquire = Acquire

export
att : Pull f o es r -> Pull f o fs (Result es r)
att = Att

newScopeIO : IO (Scope f)
newScopeIO = MkScope <$> newIORef [] <*> newIORef False <*> newIORef [] <*> pure []

prune : List (Hook f) -> IO (List (Hook f))
prune [] = pure []
prune (h :: hs) = do
  done <- readIORef h.done
  rest <- prune hs
  pure (if done then rest else h :: rest)

trackChildren : Scope Task -> List (Fiber [] ()) -> Task es ()
trackChildren _ [] = pure ()
trackChildren scope children = Async.Core.liftIO $
  traverse_ append (scope.children :: scope.ancestors)
  where
    live : List (Fiber [] ()) -> IO (List (Fiber [] ()))
    live [] = pure []
    live (child :: rest) = do
      result <- child.observe
      remaining <- live rest
      pure (case result of Nothing => child :: remaining; Just _ => remaining)

    append : IORef (List (Fiber [] ())) -> IO ()
    append ref = do
      existing <- live !(readIORef ref)
      writeIORef ref (children ++ existing)

addHook : Scope Task -> Task [] () -> Task es ()
addHook scope action = do
  done <- Async.Core.liftIO (newIORef False)
  let once = Masked $ do
        released <- Async.Core.liftIO (readIORef done)
        unless released $ do
          Async.Core.liftIO (writeIORef done True)
          action
  Async.Core.liftIO $ do
    live <- prune !(readIORef scope.hooks)
    writeIORef scope.hooks (MkHook done once :: live)

attachScope : Scope Task -> Scope Task -> Task [] () -> Task es ()
attachScope parent child cleanup = Async.Core.liftIO $ do
  live <- prune !(readIORef parent.hooks)
  writeIORef parent.hooks (MkHook child.closed cleanup :: live)

closeScope : Scope Task -> Task [] ()
closeScope scope = Masked $ do
  closed <- Async.Core.liftIO (readIORef scope.closed)
  unless closed $ do
    Async.Core.liftIO (writeIORef scope.closed True)
    children <- Async.Core.liftIO (readIORef scope.children)
    Async.Core.liftIO (writeIORef scope.children [])
    traverse_ RequestCancel children
    traverse_ (ignore . join) children
    hooks <- Async.Core.liftIO (readIORef scope.hooks)
    Async.Core.liftIO (writeIORef scope.hooks [])
    traverse_ (.action) hooks

mutual
  step : Scope Task -> Pull Task o es r -> Task es (Either r (o, Pull Task o es r))
  step scope (Val r) = pure (Left r)
  step scope (Err err) = Raise err
  step scope (Cons chunk rest) = pure (Right (chunk, rest))
  step scope (Exec action) = Masked $ do
    (result, children) <- CaptureChildren (Interruptible action)
    trackChildren scope children
    pure (Left result)
  step scope (Bind source next) = do
    value <- step scope source
    case value of
      Left r => step scope (next r)
      Right (chunk, rest) => pure (Right (chunk, Bind rest next))
  step scope (Att source) = do
    result <- attempt (step scope source)
    case result of
      Left err => pure (Left (Left err))
      Right (Left r) => pure (Left (Right r))
      Right (Right (chunk, rest)) => pure (Right (chunk, Att rest))
  step scope (Uncons source) = map Left (step scope source)
  step parent (Scoped source) = do
    fresh <- Async.Core.liftIO newScopeIO
    let scope = { ancestors := parent.children :: parent.ancestors } fresh
    attachScope parent scope (closeScope scope)
    step parent (InScope scope source)
  step parent (InScope scope source) = do
    result <- attempt (step scope source)
    case result of
      Left err => weakenErrors (closeScope scope) >> Raise err
      Right (Left r) => weakenErrors (closeScope scope) $> Left r
      Right (Right (chunk, rest)) => pure (Right (chunk, InScope scope rest))
  step scope (Acquire action release) = Masked $ do
    (resource, children) <- CaptureChildren action
    trackChildren scope children
    addHook scope (release resource)
    pure (Left resource)

  finish : Scope Task -> Pull Task Void es r -> Task es r
  finish scope source = do
    result <- step scope source
    case result of
      Left r => pure r
      Right (uninhabited, _) => absurd uninhabited

export
pull : Pull Task Void es r -> Task [] (Outcome es r)
pull source = Async.Core.bracket
  (Async.Core.liftIO newScopeIO)
  closeScope
  (\scope => map toOutcome (attempt (finish scope source)))

export
pullIn : Pull Task Void es r -> Task es r
pullIn source = do
  result <- weakenErrors (pull source)
  Async.Core.fromOutcome result

export
mpull : Monoid r => Pull Task Void es r -> Task [] r
mpull source = do
  result <- pull source
  pure (case result of Succeeded r => r; _ => neutral)
