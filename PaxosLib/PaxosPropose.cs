using Paxos.Common;
using Paxos.Message;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Protocol
{

    public enum ProposeState
    {
        Init,               // init
        QueryLastVote,      // query exiting vote
        BeginNewBallot,     // prepare commit
        Commited            // done
    }

    public class ProposePhaseResult
    {
        public ulong DecreeNo { get; set; }
        public Propose OngoingPropose { get; set; }
        public bool IsCommitted { get; set; }
        public PaxosDecree CommittedDecree { get; set; }
    }

    public class Propose
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        private readonly List<LastVoteMessage> _lastVoteMessages = new List<LastVoteMessage>();
        private readonly List<VoteMessage> _voteMessages = new List<VoteMessage>();
        private readonly List<StaleBallotMessage> _staleMessage = new List<StaleBallotMessage>();
        private ulong _clusterSize = 0;
        private ulong _lastTriedBallotNo = 0;
        private PaxosDecree _decree = null;
        private PaxosDecree _originalDecree = null;
        private ProposeState _state = ProposeState.Init;

        public Propose(ulong clusterSize)
        {
            _clusterSize = clusterSize;
        }

        public async Task<AutoLock> AcquireLock()
        {
            await _lock.WaitAsync();
            var autoLock = new AutoLock(_lock);
            return autoLock;
        }


        /// <summary>
        /// Prepare new ballot for the propose
        /// </summary>
        /// <param name="decree"></param>
        /// <returns></returns>
        public ulong PrepareNewBallot(PaxosDecree decree)
        {
            // lastvote message, vote message can touch the propose's state
            // lock it
            lock(_subscribedCompletionSource)
            {
                _lastTriedBallotNo = GetNextBallot() + 1;
                _originalDecree = decree;
                _decree = decree;

                _lastVoteMessages.Clear();
                _voteMessages.Clear();
                _staleMessage.Clear();
                _state = ProposeState.QueryLastVote;
                return _lastTriedBallotNo;
            }
        }

        /// <summary>
        /// Collect last vote received
        /// </summary>
        /// <param name="lastVoteMsg"></param>
        /// <returns></returns>
        public ulong AddLastVoteMessage(LastVoteMessage lastVoteMsg)
        {
            lock(_subscribedCompletionSource)
            {
                _lastVoteMessages.Add(lastVoteMsg);

                if (lastVoteMsg.Commited)
                {
                    _decree = lastVoteMsg.VoteDecree;
                }
                else
                {
                    var maxVoteMsg = GetMaximumVote();
                    if (maxVoteMsg != null)
                    {
                        _decree = maxVoteMsg.VoteDecree;
                    }

                }
                return (ulong)_lastVoteMessages.Count;
            }
        }

        /// <summary>
        /// Begin the new ballot
        /// </summary>
        /// <param name="ballotNo"></param>
        /// <returns></returns>
        public PaxosDecree BeginNewBallot(ulong ballotNo)
        {
            lock(_subscribedCompletionSource)
            {
                if (_lastTriedBallotNo != ballotNo)
                {
                    // last tried ballot no not match
                    return null;
                }

                var maxVoteMsg = GetMaximumVote();
                if (maxVoteMsg != null)
                {
                    _decree = maxVoteMsg.VoteDecree;
                }
                _state = ProposeState.BeginNewBallot;

                _lastVoteMessages.Clear();

                return _decree;
            }
        }

        /// <summary>
        /// Collect received vote message
        /// </summary>
        /// <param name="voteMsg"></param>
        /// <returns></returns>
        public ulong AddVoteMessage(VoteMessage voteMsg)
        {
            lock (_subscribedCompletionSource)
            {
                _voteMessages.Add(voteMsg);

                return (ulong)_voteMessages.Count;
            }
        }

        public bool Commit(ulong ballotNo)
        {
            lock (_subscribedCompletionSource)
            {
                /*
                 * commit can start from querylastvote directly because
                 * a lastvote message returned indicate the decree committed
                if (State != ProposeState.BeginNewBallot)
                {
                    return false;
                }*/

                if (ballotNo != _lastTriedBallotNo)
                {
                    // stale commit
                    return false;
                }

                if (!IsReadyToCommit())
                {
                    return false;
                }

                _state = ProposeState.Commited;
                return true;
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

        public enum NextAction { None, CollectLastVote, BeginBallot, Commit};

        public async Task<NextAction> GetNextAction()
        {
            using (var autoLock = await AcquireLock())
            {
                switch (State)
                {
                    case ProposeState.QueryLastVote:
                        foreach (var lastVote in _lastVoteMessages)
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

                        if (_lastVoteMessages.Count >= (int)_clusterSize / 2 + 1)
                        {
                            return NextAction.BeginBallot;
                        }
                        else
                        {
                            return NextAction.CollectLastVote;
                        }
                    case ProposeState.BeginNewBallot:
                        foreach (var lastVote in _lastVoteMessages)
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

                        if (_voteMessages.Count >= (int)_clusterSize / 2 + 1)
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

                if (_lastTriedBallotNo > maximumBallotNo)
                {
                    maximumBallotNo = _lastTriedBallotNo;
                }

                return maximumBallotNo;
            }
        }

        public PaxosDecree GetCommittedDecree()
        {
            lock (_subscribedCompletionSource)
            {
                // already committed
                if (State == ProposeState.Commited)
                {
                    return _decree;
                }
            }

            return null;
        }

        // propose data
        public ProposeState State => _state;

        public ulong LastTriedBallot { set { _lastTriedBallotNo = value; } get { return _lastTriedBallotNo; } }

        public TaskCompletionSource<ProposePhaseResult> Result { get; set; }

        public PaxosDecree Decree => _decree;

        public PaxosDecree OriginalDecree => _originalDecree;

        public List<LastVoteMessage> LastVoteMessages => _lastVoteMessages;

        public List<VoteMessage> VotedMessages => _voteMessages;

        /// <summary>
        /// get the maximum vote in the ammon the lastvote messages
        /// </summary>
        /// <returns></returns>
        private LastVoteMessage GetMaximumVote()
        {
            LastVoteMessage maxVoteMsg = null;
            lock (_subscribedCompletionSource)
            {
                foreach (var voteMsg in _lastVoteMessages)
                {
                    if (maxVoteMsg == null || voteMsg.VoteBallotNo > maxVoteMsg.VoteBallotNo)
                    {
                        maxVoteMsg = voteMsg;
                    }
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
        private bool IsReadyToCommit()
        {
            lock (_subscribedCompletionSource)
            {
                // decree already committed in other nodes
                if (State == ProposeState.QueryLastVote ||
                    State == ProposeState.BeginNewBallot)
                {
                    foreach (var lastVote in _lastVoteMessages)
                    {
                        if (lastVote.Commited)
                        {
                            return true;
                        }
                    }
                }

                // ready to commit
                if (State == ProposeState.BeginNewBallot && _voteMessages.Count >= (int)_clusterSize / 2 + 1)
                {
                    return true;
                }
            }

            return false;

        }

    };

    public class ProposeManager : IDisposable
    {
        private int _nextDecreeNo = 0;
        private ConcurrentDictionary<ulong, Propose> _ongoingProposes = new ConcurrentDictionary<ulong, Propose>();

        public ProposeManager(ulong baseDecreeNo)
        {
            _nextDecreeNo = (int)baseDecreeNo;
        }

        public virtual void Dispose()
        {

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

}
