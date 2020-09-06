using PineRSL.Common;
using PineRSL.Paxos.Message;
using PineRSL.Paxos.Request;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace PineRSL.Paxos.Protocol
{

    //                                                                commited returned
    //                                       |--------------------------------------------------------------------------------
    //                                       |                                                                              \|/
    // init ->  collect_last_vote -> wait_for_last_vote -> beginnewballot -> wait_for_new_ballot_result ->ReadyCommit -->  committed
    //            /|\  /|\                   |                                    | |                                       /|\
    //             |    ---------------------|                                    | |----------------------------------------|
    //             |              timeout                                         |     others has new propose and committed
    //             |                                                              |
    //             |______________________________________________________________|
    //              stale message indicate ballot already occupied by new proposer

    public enum ProposeState
    {
        Init,               // init
        QueryLastVote,      // query exiting vote
        WaitForLastVote,
        BeginNewBallot,     // prepare commit
        WaitForNewBallotVote,
        ReadyToCommit,
        Commited            // done


    }

    public class ProposePhaseResult
    {
        public ulong DecreeNo { get; set; }
        public Propose OngoingPropose { get; set; }
        public bool IsCommitted { get; set; }
        public ulong CheckpointedDecreeNo { get; set; }
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
        private ProposeState _state = ProposeState.Init;


        public class LastVoteMessageResult
        {
            public enum ResultAction { None, DecreeCommitted, DecreeCheckpointed, NewBallotReadyToBegin };
            public ResultAction Action { get; set; }
            public PaxosDecree NewBallotDecree { get; set; }
            public ulong NextBallotNo { get; set; }
            public Propose CommittedPropose { get; set; }
            public ulong CheckpointedDecreeNo { get; set; }
        }

        public class StateBallotMessageResult
        {
            public bool NeedToCollectLastVote { get; set; }
            public ulong NextBallotNo { get; set; }
            public Propose OngoingPropose { get; set; }
        }

        public class VoteMessageResult
        {
            public enum ResultAction { None, ReadyToCommit };
            public ResultAction Action { get; set; }
        };

        public Propose(ulong clusterSize)
        {
            _clusterSize = clusterSize;
        }

        public Propose(ulong clusterSize, ulong ballotNo, PaxosDecree decree, ProposeState state)
        {
            _clusterSize = clusterSize;
            _lastTriedBallotNo = ballotNo;
            _decree = decree;
            _state = state;
        }

        public async Task<AutoLock> AcquireLock()
        {
            await _lock.WaitAsync();
            var autoLock = new AutoLock(_lock);
            return autoLock;
        }

        public void PrepareCollectLastVoteMessage()
        {
            _state = ProposeState.QueryLastVote;
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
                _decree = decree;

                _lastVoteMessages.Clear();
                _voteMessages.Clear();
                _staleMessage.Clear();
                _state = ProposeState.WaitForLastVote;
                return _lastTriedBallotNo;
            }
        }

        public void PrepareWaitingLastVoteMessage(TaskCompletionSource<ProposePhaseResult> result)
        {
            Result = result;
            _state = ProposeState.WaitForLastVote;
        }

        /// <summary>
        /// Begin the new ballot
        /// </summary>
        /// <param name="ballotNo"></param>
        /// <returns></returns>
        public PaxosDecree BeginNewBallot(ulong ballotNo)
        {
            lock (_subscribedCompletionSource)
            {
                if (_lastTriedBallotNo != ballotNo)
                {
                    // last tried ballot no not match
                    return null;
                }

                var maxVoteMsg = GetMaximumVote();
                if (maxVoteMsg != null)
                {
                    _decree = new PaxosDecree(maxVoteMsg.VoteDecree);
                }
                _state = ProposeState.WaitForNewBallotVote;

                _lastVoteMessages.Clear();

                return _decree;
            }
        }

        public void PrepareWaitingVoteMessage(TaskCompletionSource<ProposePhaseResult> result)
        {
            Result = result;
            _state = ProposeState.WaitForNewBallotVote;
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

        /// <summary>
        /// Collect last vote received
        /// </summary>
        /// <param name="lastVoteMsg"></param>
        /// <returns></returns>
        public LastVoteMessageResult AddLastVoteMessage(LastVoteMessage lastVoteMsg)
        {
            lock(_subscribedCompletionSource)
            {
                if (lastVoteMsg.BallotNo != _lastTriedBallotNo)
                {
                    // stale message, drop it
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None };
                }

                if (_state == ProposeState.WaitForLastVote)
                {
                    _lastVoteMessages.Add(lastVoteMsg);
                    if (lastVoteMsg.Commited)
                    {
                        _state = ProposeState.ReadyToCommit;
                        _decree = new PaxosDecree(lastVoteMsg.VoteDecree);

                        // decree already committed
                        if (lastVoteMsg.CheckpointedDecreNo >= lastVoteMsg.DecreeNo)
                        {
                            // decree checkpointed
                            return new LastVoteMessageResult()
                            {
                                Action = LastVoteMessageResult.ResultAction.DecreeCheckpointed,
                                NextBallotNo = lastVoteMsg.BallotNo,
                                CheckpointedDecreeNo = lastVoteMsg.CheckpointedDecreNo,
                                CommittedPropose = this
                            };
                        }

                        return new LastVoteMessageResult()
                        {
                            Action = LastVoteMessageResult.ResultAction.DecreeCommitted,
                            NextBallotNo = lastVoteMsg.BallotNo,
                            CommittedPropose = this,
                            CheckpointedDecreeNo = lastVoteMsg.CheckpointedDecreNo
                        };
                    }

                    var maxVoteMsg = GetMaximumVote();
                    if (maxVoteMsg != null)
                    {
                        _decree = new PaxosDecree(maxVoteMsg.VoteDecree);
                    }

                    if ((ulong)_lastVoteMessages.Count >= _clusterSize / 2 + 1)
                    {
                        _state = ProposeState.BeginNewBallot;

                        return new LastVoteMessageResult()
                        {
                            Action = LastVoteMessageResult.ResultAction.NewBallotReadyToBegin,
                            CommittedPropose = this,
                            NextBallotNo = lastVoteMsg.BallotNo,
                            CheckpointedDecreeNo = 0
                        };
                    }

                }
                // decree committed now is returned with LastVoteMessage, so WaitForNewBallotVote
                // can also get LastVoteMessage.
                // TODO: Separate LastVoteMessage with AlreadyCheckpointed message
                else if (_state == ProposeState.WaitForNewBallotVote)
                {
                    // waitfornewballotvote
                    if (lastVoteMsg.Commited)
                    {
                        _state = ProposeState.ReadyToCommit;
                        _decree = new PaxosDecree(lastVoteMsg.VoteDecree);

                        // decree already committed
                        if (lastVoteMsg.CheckpointedDecreNo >= lastVoteMsg.DecreeNo)
                        {
                            // decree checkpointed
                            return new LastVoteMessageResult()
                            {
                                Action = LastVoteMessageResult.ResultAction.DecreeCheckpointed,
                                NextBallotNo = lastVoteMsg.BallotNo,
                                CheckpointedDecreeNo = lastVoteMsg.CheckpointedDecreNo,
                                CommittedPropose = this
                            };
                        }

                        return new LastVoteMessageResult()
                        {
                            Action = LastVoteMessageResult.ResultAction.DecreeCommitted,
                            NextBallotNo = lastVoteMsg.BallotNo,
                            CommittedPropose = this,
                            CheckpointedDecreeNo = lastVoteMsg.CheckpointedDecreNo
                        };
                    }
                }

                return new LastVoteMessageResult()
                { Action = LastVoteMessageResult.ResultAction.None };
            }
        }

        /// <summary>
        /// Collect received vote message
        /// </summary>
        /// <param name="voteMsg"></param>
        /// <returns></returns>
        public VoteMessageResult AddVoteMessage(VoteMessage voteMsg)
        {
            lock (_subscribedCompletionSource)
            {
                if (_state != ProposeState.WaitForNewBallotVote)
                {
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.None };
                }

                if (voteMsg.BallotNo != _lastTriedBallotNo)
                {
                    // stale message, drop it
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.None };
                }

                _voteMessages.Add(voteMsg);
                if ((ulong)_voteMessages.Count >= _clusterSize / 2 + 1)
                {
                    _state = ProposeState.ReadyToCommit;
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.ReadyToCommit };
                }

                return new VoteMessageResult()
                { Action = VoteMessageResult.ResultAction.None };
            }
        }

        public StateBallotMessageResult AddStaleBallotMessage(StaleBallotMessage staleMessage)
        {
            // QueryLastVote || BeginNewBallot
            lock (_subscribedCompletionSource)
            {
                if (_state != ProposeState.WaitForNewBallotVote &&
                    _state != ProposeState.WaitForLastVote)
                {
                    return new StateBallotMessageResult()
                    { NeedToCollectLastVote = false };
                }
                if (staleMessage.BallotNo != _lastTriedBallotNo)
                {
                    // stale message, drop it
                    return new StateBallotMessageResult()
                    { NeedToCollectLastVote = false };
                }
                _staleMessage.Add(staleMessage);

                _state = ProposeState.QueryLastVote;
                _lastTriedBallotNo = staleMessage.NextBallotNo;
                _decree = null; // reset decree, need to collect

                return new StateBallotMessageResult()
                { NeedToCollectLastVote = true, NextBallotNo = staleMessage.NextBallotNo, OngoingPropose = this };
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
                        return NextAction.CollectLastVote;
                    case ProposeState.BeginNewBallot:
                        return NextAction.BeginBallot;
                    case ProposeState.ReadyToCommit:
                        return NextAction.Commit;
                    default:
                        break;
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

        public List<LastVoteMessage> LastVoteMessages => _lastVoteMessages;

        public List<VoteMessage> VotedMessages => _voteMessages;

        // conters
        public TimeSpan GetProposeCostTime { get; set; }
        public TimeSpan GetProposeLockCostTime { get; set; }
        public TimeSpan PrepareNewBallotCostTime { get; set; }
        public TimeSpan BroadcastQueryLastVoteCostTime { get; set; }


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
                return State == ProposeState.ReadyToCommit;
            }
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

        public void ResetBaseDecreeNo(ulong baseDecreeNo)
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

        /// <summary>
        /// For test
        /// </summary>
        /// <param name="decreeNo"></param>
        /// <param name="propose"></param>
        public void AddPropose(ulong decreeNo, Propose propose)
        {
            RemovePropose(decreeNo);
            _ongoingProposes.TryAdd(decreeNo, propose);
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
