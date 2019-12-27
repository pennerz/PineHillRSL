using Paxos.Message;
using Paxos.Persistence;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Notebook
{
    public class DecreeBallotInfo
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        public DecreeBallotInfo()
        {
            DecreeNo = 0;
            NextBallotNo = 0;
            VotedMessage = null;
        }

        public Task AcquireLock()
        {
            return _lock.WaitAsync();
        }
        public void ReleaseLock()
        {
            _lock.Release();
        }
        public ulong DecreeNo { get; set; }
        public ulong NextBallotNo { get; set; }
        public VoteMessage VotedMessage { get; set; }
    }

    public class VoterNote
    {
        // persistent module
        private readonly IPaxosVotedBallotLog _logger;
        // voter role
        private readonly ConcurrentDictionary<ulong, DecreeBallotInfo> _ballotInfo = new ConcurrentDictionary<ulong, DecreeBallotInfo>();

        public VoterNote(IPaxosVotedBallotLog logger)
        {
            _logger = logger;
        }

        public ulong GetNextBallotNo(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = null;
            if (_ballotInfo.TryGetValue(decreeNo, out decreeBallotInfo))
            {
                return decreeBallotInfo.NextBallotNo;
            }
            return 0;
        }

        public async Task<Tuple<ulong, VoteMessage>> UpdateNextBallotNo(ulong decreeNo, ulong nextBallotNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            ulong oldNextBallotNo = 0;
            await decreeBallotInfo.AcquireLock();
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            var result = new Tuple<ulong, VoteMessage>(oldNextBallotNo, decreeBallotInfo.VotedMessage);
            if (nextBallotNo <= oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return result;
            }
            decreeBallotInfo.NextBallotNo = nextBallotNo;
            await _logger.AppendLog(decreeNo, decreeBallotInfo);
            decreeBallotInfo.ReleaseLock();
            return result;
        }

        public VoteMessage GetLastVote(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            return decreeBallotInfo.VotedMessage;
        }

        public async Task<ulong> UpdateLastVote(ulong decreeNo, ulong nextBallotNo, VoteMessage voteMsg)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            await decreeBallotInfo.AcquireLock();
            ulong oldNextBallotNo = 0;
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            if (nextBallotNo != oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return oldNextBallotNo;
            }
            decreeBallotInfo.VotedMessage = voteMsg;
            await _logger.AppendLog(decreeNo, decreeBallotInfo);
            decreeBallotInfo.ReleaseLock();
            return oldNextBallotNo;
        }

        public void Reset()
        {
            _ballotInfo.Clear();
        }

        private DecreeBallotInfo GetDecreeBallotInfo(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = null;
            if (_ballotInfo.TryGetValue(decreeNo, out decreeBallotInfo))
            {
                return decreeBallotInfo;
            }

            return null;
        }

        private DecreeBallotInfo AddDecreeBallotInfo(ulong decreeNo)
        {
            do
            {
                _ballotInfo.TryAdd(decreeNo, new DecreeBallotInfo()
                {
                     DecreeNo = decreeNo
                });
                var decreeBallotInfo = GetDecreeBallotInfo(decreeNo);
                if (decreeBallotInfo != null)
                {
                    return decreeBallotInfo;
                }
            } while (true);
        }
    }

    public enum PropserState
    {
        Init,               // init
        QueryLastVote,      // query exiting vote
        BeginNewBallot,     // prepare commit
        BeginCommit,        // commit
        Commited            // done
    }


    public class LastVoteCollectResult
    {
        //public enum Status { Committed, ReadyToNewBallot, State};
        //public Status LastVoteQueryStatus { get; set; }
        //public ulong DecreeNo { get; set; }
        //public ulong NextBallotNo { get; set; }         // for state message
        //public PaxosDecree CurrentDecree { get; set; }  // for committed decree
        public ulong DecreeNo { get; set; }
        public Propose OngoingPropose { get; set; }
        public bool IsCommitted { get; set; }
        public PaxosDecree CommittedDecree { get; set; }
    }

    public class BallotResult
    {
        //public ulong DecreeNo { get; set; }
        //public ulong NextBallotNo { get; set; }
        //public bool IsCommitted { get; set; }
        //public bool IsReadyToCommit { get; set; }
        //public bool IsStale { get; set; }
        public ulong DecreeNo { get; set; }
        public Propose OngoingPropose { get; set; }
        public bool IsCommitted { get; set; }
        public PaxosDecree CommittedDecree { get; set; }
    }

    public class AutoLock : IDisposable
    {
        private SemaphoreSlim _lock;
        public AutoLock(SemaphoreSlim ayncLock)
        {
            _lock = ayncLock;
        }

        public void Dispose()
        {
            if (_lock != null)
            {
                _lock.Release();
            }
        }
    }

    public class Propose
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        private readonly List<LastVoteMessage> _lastVoteMessages = new List<LastVoteMessage>();
        private readonly List<VoteMessage> _voteMessages = new List<VoteMessage>();
        private readonly List<StaleBallotMessage> _staleMessage = new List<StaleBallotMessage>();
        private ulong _clusterSize;
        public Propose(ulong clusterSize)
        {
            _clusterSize = clusterSize;
            State = PropserState.Init;
            LastTriedBallot = 0;
            OngoingDecree = null;
        }

        public async Task<AutoLock> AcquireLock()
        {
            await _lock.WaitAsync();
            var autoLock = new AutoLock(_lock);
            return autoLock;
        }

        /*
        public void ReleaseLock()
        {
            _lock.Release();
        }*/

        public void SubscribeCompletionNotification(TaskCompletionSource<ProposeResult> completionSource)
        {
            lock(_subscribedCompletionSource)
            {
                if (completionSource != null)
                {
                    _subscribedCompletionSource.Add(completionSource);
                }
            }
        }

        public ulong PrepareNewBallot(PaxosDecree decree)
        {
            // lastvote message, vote message can touch the propose's state
            // lock it
            lock(_subscribedCompletionSource)
            {
                LastTriedBallot++;
                OngoingDecree = decree;
                _lastVoteMessages.Clear();
                _voteMessages.Clear();
                State = PropserState.QueryLastVote;
                return LastTriedBallot;
            }
        }

        public bool BeginCommit(ulong ballotNo, PaxosDecree committingDecree)
        {
            lock(_subscribedCompletionSource)
            {
                if (ballotNo != LastTriedBallot)
                {
                    // stale commit
                    return false;
                }
                OngoingDecree = committingDecree;
                State = PropserState.BeginCommit;
                return true;
            }
        }

        public ulong AddLastVoteMessage(LastVoteMessage lastVoteMsg)
        {
            lock(_subscribedCompletionSource)
            {
                LastVoteMessages.Add(lastVoteMsg);
                var maxVoteMsg = GetMaximumVote();
                if (maxVoteMsg != null)
                {
                    OngoingDecree = maxVoteMsg.VoteDecree;
                }



                return (ulong)LastVoteMessages.Count;
            }
        }

        public PaxosDecree BeginNewBallot(ulong ballotNo)
        {
            lock(_subscribedCompletionSource)
            {
                if (LastTriedBallot != ballotNo)
                {
                    // last tried ballot no not match
                    return null;
                }
                var maxVoteMsg = GetMaximumVote();
                if (maxVoteMsg != null)
                {
                    OngoingDecree = maxVoteMsg.VoteDecree;
                }
                State = PropserState.BeginNewBallot;

                return OngoingDecree;
            }
        }

        public ulong AddVoteMessage(VoteMessage voteMsg)
        {
            lock (_subscribedCompletionSource)
            {
                VotedMessages.Add(voteMsg);

                return (ulong)VotedMessages.Count;
            }
        }

        public ulong AddStaleBallotMessage(StaleBallotMessage staleMessage)
        {
            lock (_subscribedCompletionSource)
            {
                _staleMessage.Add(staleMessage);

                return (ulong)_staleMessage.Count;
            }
        }


        private LastVoteMessage GetMaximumVote()
        {
            LastVoteMessage maxVoteMsg = null;
            foreach (var voteMsg in LastVoteMessages)
            {
                if (maxVoteMsg == null || voteMsg.VoteBallotNo > maxVoteMsg.VoteBallotNo)
                {
                    maxVoteMsg = voteMsg;
                }
            }

            if (maxVoteMsg == null)
            {
                return null;
            }

            if (maxVoteMsg.VoteBallotNo > 0)
            {
                return maxVoteMsg;
            }
            return null;
        }
        public PropserState State { get; set; }

        public ulong LastTriedBallot { get; set; }

        public PaxosDecree OngoingDecree { get; set; }

        public List<VoteMessage> VotedMessages { get { return _voteMessages; } }

        public List<LastVoteMessage> LastVoteMessages { get { return _lastVoteMessages; } }

        public List<TaskCompletionSource<ProposeResult>> CompletionEvents
        {
            get
            {
                return _subscribedCompletionSource;
            }
        }

        public TaskCompletionSource<LastVoteCollectResult> LastVoteResult { get; set; }
        public TaskCompletionSource<BallotResult> NewBallotResult { get; set; }


        public enum NextAction { None, CollectLastVote, BeginBallot, Commit};

        public async Task<NextAction> GetNextAction()
        {
            using (var autoLock = await AcquireLock())
            {
                switch (State)
                {
                    case PropserState.QueryLastVote:
                        foreach (var lastVote in LastVoteMessages)
                        {
                            if (lastVote.Commited)
                            {
                                return NextAction.Commit;
                            }
                        }

                        if (_staleMessage.Count > 0)
                        {
                            return NextAction.CollectLastVote;
                        }

                        if (LastVoteMessages.Count > (int)_clusterSize / 2 + 1)
                        {
                            return NextAction.BeginBallot;
                        }
                        else
                        {
                            return NextAction.CollectLastVote;
                        }
                    case PropserState.BeginNewBallot:
                        foreach (var lastVote in LastVoteMessages)
                        {
                            if (lastVote.Commited)
                            {
                                return NextAction.Commit;
                            }
                        }
                        if (_staleMessage.Count > 0)
                        {
                            return NextAction.CollectLastVote;
                        }

                        if (VotedMessages.Count > (int)_clusterSize / 2 + 1)
                        {
                            return NextAction.Commit;
                        }
                        else
                        {
                            return NextAction.BeginBallot;
                        }
                }
            }

            return NextAction.None;
        }

        public ulong GetNextBallot()
        {
            lock(_subscribedCompletionSource)
            {
                ulong maximumBallotNo = 0;
                foreach(var staleMsg in _staleMessage)
                {
                    if (staleMsg.NextBallotNo > maximumBallotNo)
                    {
                        maximumBallotNo = staleMsg.NextBallotNo;
                    }
                }

                if (LastTriedBallot > maximumBallotNo)
                {
                    maximumBallotNo = LastTriedBallot;
                }

                return maximumBallotNo;
            }
        }

        public PaxosDecree GetCommittedDecree()
        {
            lock (_subscribedCompletionSource)
            {
                foreach(var lastVote in _lastVoteMessages)
                {
                    if (lastVote.Commited)
                    {
                        return lastVote.VoteDecree;
                    }
                }

                if (_voteMessages.Count > (int)_clusterSize/2+1)
                {
                    return OngoingDecree;
                }
            }

            return null;
        }

    }

    public class ProposeManager
    {
        private int _nextDecreeNo = 0;
        private ConcurrentDictionary<ulong, Propose> _ongoingProposes = new ConcurrentDictionary<ulong, Propose>();

        public ProposeManager(ulong baseDecreeNo)
        {
            _nextDecreeNo = (int)baseDecreeNo;
        }

        public ulong GetNextDecreeNo()
        {
            return (ulong)Interlocked.Increment(ref _nextDecreeNo);
        }

        public Propose GetOngoingPropose(ulong decreeNo)
        {
            Propose propose = null;
            if (!_ongoingProposes.TryGetValue(decreeNo, out propose))
            {
                return null;
            }

            return propose;
        }

        public Propose AddPropose(ulong decreeNo, ulong clusterSize)
        {
            do
            {
                var propose = GetOngoingPropose(decreeNo);
                if (propose != null)
                {
                    return propose;
                }
                propose = new Propose(clusterSize);
                _ongoingProposes.TryAdd(decreeNo, propose);
            }while(true);
        }

        public void RemovePropose(ulong decreeNo)
        {
            Propose propose = null;
            _ongoingProposes.TryRemove(decreeNo, out propose);
        }

        public void Reset()
        {
            _ongoingProposes.Clear();
        }
    }

    public class ProposerNote
    {
        // proposer role
        //private ConcurrentDictionary<ulong, Propose> _decreeState = new ConcurrentDictionary<ulong, Propose>();
        //private Ledger _ledger;
        private readonly ConcurrentDictionary<ulong, PaxosDecree> _committedDecrees = new ConcurrentDictionary<ulong, PaxosDecree>();
        private IPaxosCommitedDecreeLog _logger;

        public ProposerNote(IPaxosCommitedDecreeLog logger)
        {
            if (logger == null)
            {
                throw new ArgumentNullException("IPaxosCommitedDecreeLogger");
            }
            _logger = logger;
        }

        /*
        /// <summary>
        /// When try to propose a new decree, get a decree no first
        /// </summary>
        /// <returns></returns>
        public ulong GetNewDecreeNo(ulong clusterSize)
        {
            ulong nextDecreeNo = 0;

            // new decree no generation is syncrhonized
            lock(_logger)
            {
                nextDecreeNo = GetMaximumCommittedDecreeNoNeedLock() + 1;
                var maxOngoingDecreeNo = GetMaxOngoingDecreeNo();
                if (nextDecreeNo < maxOngoingDecreeNo + 1)
                {
                    nextDecreeNo = maxOngoingDecreeNo + 1;
                }
                var propose = AddPropose(nextDecreeNo, clusterSize);
            }

            return nextDecreeNo;
        }
        */
        /// <summary>
        /// Get the maximum committed decree no
        /// </summary>
        /// <returns></returns>
        public ulong GetMaximumCommittedDecreeNo()
        {
            ulong maximumCommittedDecreeNo = 0;
            lock(_logger)
            {
                maximumCommittedDecreeNo = GetMaximumCommittedDecreeNoNeedLock();
            }
            return maximumCommittedDecreeNo;
        }
        

        /// <summary>
        /// Get the committed decree
        /// </summary>
        /// <param name="decreeNo"></param>
        /// <returns></returns>
        public Task<PaxosDecree> GetCommittedDecree(ulong decreeNo)
        {
            // committed dcree can never be changed
            PaxosDecree committedDecree = null;
            if (!_committedDecrees.TryGetValue(decreeNo, out committedDecree))
            {
                committedDecree = null;
            }
            return Task.FromResult(committedDecree);
        }

        /*
        public async Task<Propose> Commit(ulong decreeNo, PaxosDecree committedDecree)
        {
            // write the decree to ledge
            await CommitDecreeInternal(decreeNo, committedDecree);

            // propose commited, remove it from ongoing proposes
            Propose propose = null;
            while (!_decreeState.TryRemove(decreeNo, out propose)) ;

            return propose;
        }*/

        public async Task CommitDecree(ulong decreeNo, PaxosDecree commitedDecree)
        {
            await CommitDecreeInternal(decreeNo, commitedDecree);
        }


        /*
        public ulong GetMaxOngoingDecreeNo()
        {
            if (_decreeState.Count == 0)
            {
                return 0;
            }
            return _decreeState.LastOrDefault().Key;
        }

        public Propose GetPropose(ulong decreeNo)
        {
            Propose propose = null;
            if (_decreeState.TryGetValue(decreeNo, out propose))
            {
                return propose;
            }

            return null;
        }

        public Propose AddPropose(ulong decreeNo, ulong clusterSize)
        {
            do
            {
                _decreeState.TryAdd(decreeNo, new Propose(clusterSize));
                var propose = GetPropose(decreeNo);
                if (propose != null)
                {
                    return propose;
                }
            } while (true);
        }

        public void UpdatePropose(ulong decreeNo, Propose propose)
        {
            _decreeState.AddOrUpdate(decreeNo, propose, (key, oldValue) => propose);
        }
        */
        public List<KeyValuePair<ulong, PaxosDecree>> GetFollowingCommittedDecress(ulong beginDecreeNo)
        {
            var committedDecrees = new List<KeyValuePair<ulong, PaxosDecree>>();
            lock (_logger)
            {
                for (ulong decreeNo = beginDecreeNo; decreeNo <= GetMaximumCommittedDecreeNo(); decreeNo++)
                {
                    PaxosDecree committedDecree = null;
                    if (_committedDecrees.TryGetValue(decreeNo, out committedDecree))
                    {
                        committedDecrees.Add(new KeyValuePair<ulong, PaxosDecree>(decreeNo, committedDecree));
                    }
                }
            }
            return committedDecrees.Count > 0 ? committedDecrees : null;
        }
        public ulong GetCommittedDecreeCount()
        {
            return (ulong)_committedDecrees.Count;
        }

        public void Clear()
        {
            _committedDecrees.Clear();
        }

        /*
        public void Reset()
        {
            _decreeState.Clear();
        }

        public void ClearDecree(ulong decreeNo)
        {
            Propose propose = null;
            _decreeState.Remove(decreeNo, out propose);
        }*/
        private ulong GetMaximumCommittedDecreeNoNeedLock()
        {
            return _committedDecrees.LastOrDefault().Key;
        }
        

        private async Task CommitDecreeInternal(ulong decreeNo, PaxosDecree decree)
        {
            // Paxos protocol already make sure the consistency.
            // So the decree will be idempotent, just write it directly

            await _logger.AppendLog(decreeNo, decree);

            // add it in cache
            lock (_committedDecrees)
            {
                do
                {
                    if (_committedDecrees.ContainsKey(decreeNo))
                    {
                        return;
                        //throw new Exception("decree already committed");
                    }
                    if (_committedDecrees.TryAdd(decreeNo, decree))
                    {
                        return;
                    }
                } while (true);
            }
        }

    }

}
