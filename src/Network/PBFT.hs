

module Network.PBFT where


import           Protolude
import qualified Data.Map     as M
import qualified Data.Set     as S


type family NodeId          app :: *
type family ClientId        app :: *

-- | The clients issues operations that are committed/executed by the replicas. For instance
--   on a crypto-currency, this would be a transaction
type family PbftOperation   app :: *

type family PbftResult      app :: *

-- | Called `timestampt` on the white paper, is a strictly monotonic kind of counter
type family PbftNonce       app :: *

-- | `PbftView` changes are carried out when it appears that the primary has failed
newtype PbftView   = PbftView   Int deriving(Eq,Ord,Num)

-- | On the original paper each replica weights 1, but we allow any positive integer as
--   weights (As is common)
newtype NodeWeight = NodeWeight Int deriving newtype(Eq,Ord,Num,Integral,Real,Enum)

data ClientRequest app = ClientRequest
      { _crOperation :: PbftOperation app
      , _crNonce     :: PbftNonce     app
      , _crClientId  :: ClientId      app
      } deriving(Generic)

data ReplicaReply app = ReplicaReply
      { _rrCurrentView :: PbftView
      , _rrNonce       :: PbftNonce   app
      , _rrClientId    :: ClientId    app
      , _rrNodeId      :: NodeId      app
      , _rrResult      :: PbftResult  app
      } deriving(Generic)

data PbftClientParams m app = PbftClientParams
      { _pccNodes             :: Map (NodeId app) NodeWeight
      , _pccClientId          :: ClientId     app
      , _pccPrimary           :: NodeId       app
      , _pccCurrentTime       :: m (PbftNonce app)

      , _pccBrodcastMessageTo :: [NodeId app] -> ClientRequest app 
                              -> m ( MStream m (ReplicaReply app)
                                   , m ()
                                   ) 
      } deriving(Generic)


-- | Run something till there's no more or somethhng unexpected, such a timeout or an network connection drop happens.
data MStream m a = EmptyStream 
                 | ContinueMStream a (m (MStream m a)) 
                 deriving(Generic)

data PbftError   = NotEnoughReplies

type ClientState app  = Map (PbftResult app,(PbftView,PbftNonce app)) 
                            (Set (NodeId app))

pbftExecute :: forall m app . ( Monad m
                              , Ord (PbftResult app)
                              , Ord (PbftNonce  app)
                              , Ord (NodeId     app)
                              , Eq  (ClientId   app)

                              ) => PbftClientParams m app 
                                -> PbftOperation app
                                -> m (Either PbftError (PbftResult app))

pbftExecute PbftClientParams{..} op = do nonce   <- _pccCurrentTime
                                         result1 <- step nonce [_pccPrimary]
                                         case result1 of
                                             Left _ -> step nonce $ M.keys _pccNodes
                                             _      -> return result1


  where
    step :: PbftNonce app
         -> [NodeId app] 
         -> m (Either PbftError (PbftResult app))

    step t replicas = do (stream, stop) <- _pccBrodcastMessageTo replicas ClientRequest
                                              { _crOperation = op
                                              , _crNonce     = t
                                              , _crClientId  = _pccClientId
                                              }

                         waitEnoughMsgs (M.fromList []) stream <* stop
    

    waitEnoughMsgs :: ClientState app 
                   -> MStream m (ReplicaReply app) 
                   -> m (Either PbftError (PbftResult app))

    waitEnoughMsgs _   EmptyStream                = return $ Left NotEnoughReplies
    waitEnoughMsgs acc (ContinueMStream x stream) = case addCheckMsg x acc of
                                                     Left  acc'          -> waitEnoughMsgs acc' =<< stream 
                                                     Right repeatedReply -> return $ Right repeatedReply

    requiredWeight = (sum (toList _pccNodes) `div` 3) + 1


    addCheckMsg :: ReplicaReply app
                -> ClientState app 
                -> Either (ClientState app) (PbftResult app)
    
    addCheckMsg ReplicaReply{..} acc 
            | _rrClientId /= _pccClientId = Left acc
            | otherwise                   = let key   = (_rrResult,(_rrCurrentView, _rrNonce))
                                                value = S.singleton _rrNodeId
                                                acc'  = M.insertWith (S.union) key value acc

                                             in case M.lookup key acc' of
                                                 Just value' | enoughWeight value' -> Right _rrResult
                                                 _                                 -> Left  acc'

    enoughWeight xs = sum [ fromMaybe 0 $ M.lookup x _pccNodes | x <- toList xs] >= requiredWeight
