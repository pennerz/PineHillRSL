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
        public enum Status { Committed, ReadyToNewBallot, State};
        public Status LastVoteQueryStatus { get; set; }
        public ulong DecreeNo { get; set; }
        public ulong NextBallotNo { get; set; }         // for state message
        public PaxosDecree CurrentDecree { get; set; }  // for committed decree
    }

    public class BallotResult
    {
        public ulong DecreeNo { get; set; }
        public ulong NextBallotNo { get; set; }
        public bool IsCommitted { get; set; }
        public bool IsReadyToCommit { get; set; }
        public bool IsStale { get; set; }
    }

    public class Propose
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        private readonly List<LastVoteMessage> _lastVoteMessages = new List<LastVoteMessage>();
        private readonly List<VoteMessage> _voteMessages = new List<VoteMessage>();

        public Propose()
        {
            State = PropserState.Init;
            LastTriedBallot = 0;
            OngoingDecree = null;
        }

        public Task AcquireLock()
        {
            return _lock.WaitAsync();
        }

        public void ReleaseLock()
        {
            _lock.Release();
        }

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

    }

    public class ProposerNote
    {
        // proposer role
        private ConcurrentDictionary<ulong, Propose> _decreeState = new ConcurrentDictionary<ulong, Propose>();
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

        /// <summary>
        /// When try to propose a new decree, get a decree no first
        /// </summary>
        /// <returns></returns>
        public ulong GetNewDecreeNo()
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
                var propose = AddPropose(nextDecreeNo);
            }

            return nextDecreeNo;
        }

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

        public async Task<Propose> Commit(ulong decreeNo, PaxosDecree committedDecree)
        {
            // write the decree to ledge
            await CommitDecreeInternal(decreeNo, committedDecree);

            // propose commited, remove it from ongoing proposes
            Propose propose = null;
            while (!_decreeState.TryRemove(decreeNo, out propose)) ;

            return propose;
        }

        public async Task CommitDecree(ulong decreeNo, PaxosDecree commitedDecree)
        {
            await CommitDecreeInternal(decreeNo, commitedDecree);
        }


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

        public Propose AddPropose(ulong decreeNo)
        {
            do
            {
                _decreeState.TryAdd(decreeNo, new Propose());
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

        public void Reset()
        {
            _decreeState.Clear();
        }

        public void ClearDecree(ulong decreeNo)
        {
            Propose propose = null;
            _decreeState.Remove(decreeNo, out propose);
        }

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
