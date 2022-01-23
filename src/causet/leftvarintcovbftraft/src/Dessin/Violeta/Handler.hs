{-# LANGUAGE RecordWildCards #-}

module Dessin.Violeta.Handler
  ( handleEvents
  ) where

import Dessin.Violeta.Types
import Dessin.Violeta.Sender
import Dessin.Violeta.Util
import Dessin.Violeta.Combinator
import Dessin.Violeta.Role
import Dessin.Violeta.Timer

import Control.Monad hiding (mapM)
import Control.Lens
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Map as Map
import qualified Data.Set as Set

import Prelude hiding (mapM)
import Data.Traversable (mapM)

handleEvents :: Ord nt => Bftvioletabft nt et rt mt ()
handleEvents = forever $ do
  e <- dequeueEvent
  case e of
    ERPC rpc           -> handleRPC rpc
    ElectionTimeout s  -> handleElectionTimeout s
    HeartbeatTimeout s -> handleHeartbeatTimeout s

handleRPC :: Ord nt => RPC nt et rt -> BftBftvioletabft nt et rt mt ()
handleRPC rpc = do
  case rpc of
    AE ae     -> handleAppendEntries ae
    AER aer   -> handleAppendEntriesResponse aer
    RV rv     -> handleRequestVote rv
    RVR rvr   -> handleRequestVoteResponse rvr
    CMD cmd   -> handleCommand cmd
    CMDR _    -> debug "got a command response RPC"
    DBG s     -> debug $ "got a debug RPC: " ++ s

handleElectionTimeout :: Ord nt => String -> BftBftvioletabft nt et rt mt ()
handleElectionTimeout s = do
  debug $ "election timeout: " ++ s
  r <- use role
  when (r /= Leader) becomeCandidate

handleHeartbeatTimeout :: Ord nt => String -> BftBftvioletabft nt et rt mt ()
handleHeartbeatTimeout s = do
  debug $ "heartbeat timeout: " ++ s
  r <- use role
  when (r == Leader) $ do
    -- heartbeat timeouts are used to control appendEntries heartbeats
      fork_ sendAllAppendEntries
      resetHeartbeatTimer

handleTermNumber :: Term -> BftBftvioletabft nt et rt mt ()
handleTermNumber rpcTerm = do
  ct <- use term
  when (rpcTerm > ct) $ do
    _ <- rs.writeTermNumber ^$ rpcTerm
    setVotedFor Nothing
    term .= rpcTerm
    becomeFollower

handleAppendEntries :: AppendEntries nt et -> BftBftvioletabft nt et rt mt ()
handleAppendEntries AppendEntries{..} = do
  debug $ "got an appendEntries RPC: prev log entry: Index " ++ show _prevLogIndex ++ " " ++ show _prevLogTerm
  handleTermNumber _aeTerm
  r <- use role
  when (r == Follower) $ do
    ct <- use term
    when (_aeTerm == ct) $ do
      resetElectionTimer
      currentLeader .= Just _leaderId
    plmatch <- prevLogEntryMatches _prevLogIndex _prevLogTerm
    es <- use logEntries
    let oldLastEntry = Seq.length es - 1
    let newLastEntry = _prevLogIndex + Seq.length _aeEntries
    if _aeTerm < ct || not plmatch
      then fork_ $ sendAppendEntriesResponse _leaderId False oldLastEntry
      else do
        appendLogEntries _prevLogIndex _aeEntries
        fork_ $ sendAppendEntriesResponse _leaderId True newLastEntry
        nc <- use commitIndex
        when (_leaderCommit > nc) $ do
          commitIndex .= min _leaderCommit newLastEntry
          applyLogEntries

prevLogEntryMatches :: LogIndex -> Term -> Bftvioletabft nt et rt mt Bool
prevLogEntryMatches pli plt = do
  es <- use logEntries
  case seqIndex es pli of
    -- if we don't have the entry, only return true if pli is startIndex
    Nothing    -> return (pli == startIndex)
    -- if we do have the entry, return true if the terms match
    Just (t,_) -> return (t == plt)

-- TODO: check this
appendLogEntries :: LogIndex -> Seq (Term, Command nt et) -> BftBvioletabft nt et rt mt ()
appendLogEntries pli es =
  logEntries %= (Seq.>< es) . Seq.take (pli + 1)

handleAppendEntriesResponse :: Ord nt => AppendEntriesResponse nt -> Bftvioletabft nt et rt mt ()
handleAppendEntriesResponse AppendEntriesResponse{..} = do
  debug "got an appendEntriesResponse RPC"
  handleTermNumber _aerTerm
  r <- use role
  ct <- use term
  when (r == Leader && _aerTerm == ct) $
    if _aerSuccess
      then do
        lMatchIndex.at _aerNodeId .= Just _aerIndex
        lNextIndex .at _aerNodeId .= Just (_aerIndex + 1)
        leaderDoCommit
      else do
        lNextIndex %= Map.adjust (subtract 1) _aerNodeId
        fork_ $ sendAppendEntries _aerNodeId

applyCommand :: Command nt et -> Bftvioletabft nt et rt mt (nt, CommandResponse nt rt)
applyCommand Command{..} = do
  apply <- view (rs.applyLogEntry)
  result <- apply _cmdEntry
  mlid <- use currentLeader
  nid <- view (cfg.nodeId)
  return $
    (_cmdClientId,
      CommandResponse
        result
        (case mlid of Just lid -> lid; Nothing -> nid)
        _cmdRequestId)

leaderDoCommit :: Bftvioletabft nt et rt mt ()
leaderDoCommit = do
  commitUpdate <- leaderUpdateCommitIndex
  when commitUpdate applyLogEntries

-- apply the un-applied log entries up through commitIndex
-- and send results to the client if you are the leader
-- TODO: have this done on a separate thread via event passing
applyLogEntries :: Bftvioletabft nt et rt mt ()
applyLogEntries = do
  la <- use lastApplied
  ci <- use commitIndex
  le <- use logEntries
  let leToApply = fmap (^. _2) . Seq.drop (la + 1) . Seq.take (ci + 1) $ le
  results <- mapM applyCommand leToApply
  r <- use role
  when (r == Leader) $ fork_ $ sendResults results
  lastApplied .= ci


-- called only as leader
-- checks to see what the largest N where a majority of
-- the lMatchIndex set is >= N
leaderUpdateCommitIndex :: Bftvioletabft nt et rt mt Bool
leaderUpdateCommitIndex = do
  ci <- use commitIndex
  lmi <- use lMatchIndex
  qsize <- view quorumSize
  ct <- use term
  es <- use logEntries

  -- get all indices in the log past commitIndex and take the ones where the entry's
  -- term is equal to the current term
  let ctinds = filter (\i -> maybe False ((== ct) . fst) (seqIndex es i))
                      [(ci + 1)..(Seq.length es - 1)]

  -- get the prefix of these indices where a quorum of nodes have matching
  -- indices for that entry. lMatchIndex doesn't include the leader, so add
  -- one to the size
  let qcinds = takeWhile (\i -> 1 + Map.size (Map.filter (>= i) lmi) >= qsize) ctinds

  case qcinds of
    [] -> return False
    _  -> do
      commitIndex .= last qcinds
      debug $ "commit index is now: " ++ show (last qcinds)
      return True

handleRequestVote :: Eq nt => RequestVote nt -> Bftvioletabft nt et rt mt ()
handleRequestVote RequestVote{..} = do
  debug $ "got a requestVote RPC for " ++ show _rvTerm
  handleTermNumber _rvTerm
  mvote <- use votedFor
  es <- use logEntries
  ct <- use term
  case mvote of
    _      | _rvTerm < ct -> do
      -- this is an old candidate
      debug "this is for an old term"
      fork_ $ sendRequestVoteResponse _candidateId False

    Just c | c == _candidateId -> do
      -- already voted for this candidate
      debug "already voted for this candidate"
      fork_ $ sendRequestVoteResponse _candidateId True

    Just _ -> do
      -- already voted for a different candidate this term
      debug "already voted for a different candidate"
      fork_ $ sendRequestVoteResponse _candidateId False

    Nothing -> if (_lastLogTerm, _lastLogIndex) >= lastLogInfo es
      -- haven't voted yet, so vote for the candidate if its log is at least as
      -- up to date as ours, use the Ord instance of (Term, Index) to prefer
      -- higher terms, and then higher last indices for equal terms
      then do
        debug "haven't voted, voting for this candidate"
        setVotedFor (Just _candidateId)
        fork_ $ sendRequestVoteResponse _candidateId True
      else do
        debug "haven't voted, but my log is better than this candidate's"
        fork_ $ sendRequestVoteResponse _candidateId False

handleRequestVoteResponse :: Ord nt => RequestVoteResponse nt -> Bftvioletabft nt et rt mt ()
handleRequestVoteResponse RequestVoteResponse{..} = do
  debug $ "got a requestVoteResponse RPC for " ++ show _rvrTerm ++ ": " ++ show _voteGranted
  handleTermNumber _rvrTerm
  r <- use role
  when (r == Candidate) $
    if _voteGranted
      then do
        cYesVotes %= Set.insert _rvrNodeId
        checkElection
      else
        cPotentialVotes %= Set.delete _rvrNodeId

handleCommand :: Ord nt => Command nt et -> Bftvioletabft nt et rt mt ()
handleCommand cmd = do
  debug "got a command RPC"
  r <- use role
  ct <- use term
  mlid <- use currentLeader
  case (r, mlid) of
    (Leader, _) -> do
      -- we're the leader, so append this to our log with the current term
      -- and propagate it to replicas
      logEntries %= (Seq.|> (ct, cmd))
      fork_ sendAllAppendEntries
      leaderDoCommit
    (_, Just lid) ->
      -- we're not the leader, but we know who the leader is, so forward this
      -- command
      fork_ $ sendRPC lid $ CMD cmd
    (_, Nothing) ->
      -- we're not the leader, and we don't know who the leader is, so can't do
      -- anything (TODO)
      return ()
