using Paxos.Network;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Persistence;
using Paxos.Node;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace Paxos.Protocol
{

    public interface IPaxos
    {
        Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree);
        Task Checkpoint();
    }

    public interface IPaxosStateMachine
    {
    }

    public class PaxosCluster
    {
        private List<NodeInfo> members = new List<NodeInfo>();

        public List<NodeInfo> Members
        {
            get
            {
                return members;
            }
        }
    }

    public class DecreeLock
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private DateTime _lastTouched = DateTime.Now;

        public DecreeLock()
        {
            ReferenceCount = 0;
        }

        public Task AcquireLock()
        {
            return _lock.WaitAsync();
        }

        public void ReleaseLock()
        {
            _lastTouched = DateTime.Now;
            _lock.Release();
        }

        public ulong ReferenceCount { get; set; }

        public DateTime LastTouchedTime { get { return _lastTouched; } }
    }

    public class DecreeLockWrapper
    {
        private DecreeLock _decreeLock;

        public DecreeLockWrapper(DecreeLock decreeLock)
        {
            if (decreeLock == null)
            {
                throw new ArgumentNullException("Decree Lock");
            }
            _decreeLock = decreeLock;
            _decreeLock.ReferenceCount++;
        }

        public Task AcquireLock()
        {
            return _decreeLock.AcquireLock();
        }

        public void ReleaseLock()
        {
            _decreeLock.ReleaseLock();
        }

        ~DecreeLockWrapper()
        {
            _decreeLock.ReferenceCount--;
        }
    }

    public class DecreeLockManager
    {
        private Dictionary<ulong, DecreeLock> _decreeLocks = new Dictionary<ulong, DecreeLock>();
        private Task _gcTask;

        public DecreeLockManager()
        {
            GCThresholdInSeconds = 30;
            Stop = false;
            _gcTask = Task.Run(async () =>
            {
                while (!Stop)
                {
                    await Task.Delay(10000);
                    var now = DateTime.Now;

                    lock(_decreeLocks)
                    {
                        foreach (var decreeLock in _decreeLocks)
                        {
                            if (decreeLock.Value.ReferenceCount > 0)
                            {
                                continue;
                            }
                            var lastTouchedTime = decreeLock.Value.LastTouchedTime;
                            var diff = lastTouchedTime - now;
                            if (diff.TotalSeconds < GCThresholdInSeconds)
                            {
                                continue;
                            }
                            DecreeLock decreeTransaction = null;
                            _decreeLocks.Remove(decreeLock.Key, out decreeTransaction);
                        }

                    }
                }
            });
        }

        public DecreeLockWrapper GetDecreeLock(ulong decreeNo)
        {
            lock(_decreeLocks)
            {
                do
                {
                    DecreeLock decreeLock = null;
                    if (_decreeLocks.TryGetValue(decreeNo, out decreeLock))
                    {
                        return new DecreeLockWrapper(decreeLock);
                    }
                    _decreeLocks.TryAdd(decreeNo, new DecreeLock());
                } while (true);

            }
        }


        public ulong GCThresholdInSeconds { get; set; }

        public bool Stop { get; set; }
    }

    /// <summary>
    /// Event Received          Action
    /// LastVote                Return last vote if the new ballot no bigger than NextBallotNo recorded
    /// BeginNewBallot          If the new ballotNo is equal to NextBallotNo recorded,vote for the ballot
    ///                         and save the vote as last vote
    /// CommitDecree            Save the decree
    /// </summary>
    ///
    public class VoterRole
    {
        private readonly IMessageTransport _messageTransport;
        private readonly DecreeLockManager _decreeLockManager;

        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly VoterNote _note;
        private readonly Ledger _ledger;

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IMessageTransport messageTransport,
            DecreeLockManager decreeLockManager,
            VoterNote paxoserNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (messageTransport == null) throw new ArgumentNullException("IMessageTransport");
            if (paxoserNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");
            if (decreeLockManager == null) throw new ArgumentNullException("decree lock manager");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _messageTransport = messageTransport;
            _note = paxoserNote;
            _ledger = ledger;
            _decreeLockManager = decreeLockManager;
        }

        public async Task DeliverNextBallotMessage(NextBallotMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            await decreeLock.AcquireLock();

            // check if the decree committed
            LastVoteMessage lastVoteMsg = null;

            try
            {
                var commitedDecree = _ledger.GetCommittedDecree(msg.DecreeNo);
                if (commitedDecree != null)
                {
                    lastVoteMsg = new LastVoteMessage()
                    {
                        TargetNode = msg.SourceNode,
                        Commited = true,
                        BallotNo = msg.BallotNo,
                        DecreeNo = msg.DecreeNo,
                        VoteBallotNo = 0, // not applicable
                        VoteDecree = commitedDecree,
                        CommittedDecrees = _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                    };

                    await _messageTransport.SendMessage(lastVoteMsg);
                    return;
                }

                var result = await _note.UpdateNextBallotNo(msg.DecreeNo, msg.BallotNo);
                var oldBallotNo = result.Item1;
                var lastVote = result.Item2;
                if (oldBallotNo >= msg.BallotNo)
                {
                    // do not response the ballotNo < current nextBallotNo
                    var staleBallotMsg = new StaleBallotMessage()
                    {
                        NextBallotNo = oldBallotNo
                    };
                    staleBallotMsg.TargetNode = msg.SourceNode;
                    staleBallotMsg.BallotNo = msg.BallotNo;
                    staleBallotMsg.DecreeNo = msg.DecreeNo;

                    await _messageTransport.SendMessage(staleBallotMsg);

                    return;
                }

                // it could be possible when this lastvotemessage is sent out
                // a new ballot has been voted, in that case, when this LastVoteMessage
                // returned, two case
                // 1. the new ballot is triggered by same node as this one, in this case
                //    this LastVoteMessage will be abandoned
                // 2. the new ballot is triggered by other node, in this case, the node
                //    trigger this ballot will accept this last LastVoteMessage, and it

                // send back the last vote information
                lastVoteMsg = new LastVoteMessage();
                lastVoteMsg.TargetNode = msg.SourceNode;
                lastVoteMsg.BallotNo = msg.BallotNo;
                lastVoteMsg.DecreeNo = msg.DecreeNo;
                lastVoteMsg.VoteBallotNo = lastVote != null ? lastVote.BallotNo : 0;
                lastVoteMsg.VoteDecree = lastVote?.VoteDecree;

                await _messageTransport.SendMessage(lastVoteMsg);

            }
            finally
            {
                decreeLock.ReleaseLock();
            }
        }

        public async Task DeliverBeginBallotMessage(BeginBallotMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            VoteMessage voteMsg = null;
            ulong oldNextBallotNo = 0;
            ulong nextBallotNo = 0;
            await decreeLock.AcquireLock();
            try
            {
                if (_ledger.GetCommittedDecree(msg.DecreeNo) != null)
                {
                    return;
                }

                nextBallotNo = _note.GetNextBallotNo(msg.DecreeNo);
                if (msg.BallotNo > nextBallotNo)
                {
                    // should not happend, send alert
                    return;
                }

                // vote this ballot
                voteMsg = new VoteMessage();
                voteMsg.TargetNode = msg.SourceNode;
                voteMsg.BallotNo = msg.BallotNo;
                voteMsg.DecreeNo = msg.DecreeNo;
                voteMsg.VoteDecree = msg.Decree;

                // save last vote
                oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, voteMsg);
            }
            finally
            {
                decreeLock.ReleaseLock();
            }

            if (oldNextBallotNo > msg.BallotNo)
            {
                var staleBallotMsg = new StaleBallotMessage()
                {
                    NextBallotNo = nextBallotNo
                };
                staleBallotMsg.TargetNode = msg.SourceNode;
                staleBallotMsg.BallotNo = msg.BallotNo;
                staleBallotMsg.DecreeNo = msg.DecreeNo;

                await _messageTransport.SendMessage(staleBallotMsg);

                return;
            }
            if (oldNextBallotNo < msg.BallotNo)
            {
                // cant be, send alert
                return;
            }

            // deliver the vote message
            await _messageTransport.SendMessage(voteMsg);
        }

        public async Task DeliverSuccessMessage(SuccessMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            await decreeLock.AcquireLock();
            // save it to ledge
            await _ledger.CommitDecree(msg.DecreeNo, msg.Decree);
            //_note.ClearDecree(msg.DecreeNo);

            decreeLock.ReleaseLock();
        }
    }

    /// <summary>
    /// Three phase commit (3PC)
    /// 1. query existing vote
    /// 2. prepare commit
    /// 3. commit
    /// 
    ///     CurrentState            Event                   NextState               Action
    ///     Init                    Propose                 QueryLastVote           Query lastvote from cluster
    ///     QueryLastVote           ReceiveLastVote         BeginNewBallot(enough)  Send BeginNewBallot to quorum
    ///                                                     QueryLastVote(x)        Nothing
    ///                                                     BeginCommit(Committed)   Send commit to missing decree node
    ///     QueryLastVote           Timeout                 QueryLastVote           Query lastvote with new ballotNo
    ///     BeginNewBallot          ReceiveVote             BeginCommit(enough)     Send commit to all nodes
    ///                                                     ReceiveVote(x)          Nothing
    ///     BeginNewBallot          Timeout                 QueryLastVote           Send BeginNewBallot with a new ballotNo
    ///     BeginCommit             Response                Committed(enough)        Call subscriber
    ///                                                     BeginCommit(x)
    ///     BeginCommit             Timeout                 BeginCommit             Send commit to all nodes
    ///     
    /// </summary>

    public class ProposerRole
    {
        private readonly IMessageTransport _messageTransport;
        private readonly DecreeLockManager _decreeLockManager;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly Ledger _ledger;

        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IMessageTransport messageTransport,
            DecreeLockManager decreeLockManager,
            ProposerNote proposerNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (messageTransport == null) throw new ArgumentNullException("IMessageTransport");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");
            if (ledger == null) throw new ArgumentNullException("ledger");
            if (decreeLockManager == null) throw new ArgumentNullException("decreeLock manager");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _messageTransport = messageTransport;
            _proposerNote = proposerNote;
            _ledger = ledger;
            _decreeLockManager = decreeLockManager;

            Stop = false;
        }

        public bool Stop { get; set; }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            DecreeReadResult result = null;
            var maximumCommittedDecreeNo = _proposerNote.GetMaximumCommittedDecreeNo();
            if (decreeNo > maximumCommittedDecreeNo)
            {
                 result = new DecreeReadResult()
                {
                    IsFound = false,
                    MaxDecreeNo = maximumCommittedDecreeNo,
                    Decree = null
                };
                return result;
            }

            var decree = await _proposerNote.GetCommittedDecree(decreeNo);
            result = new DecreeReadResult()
            {
                IsFound = true,
                MaxDecreeNo = maximumCommittedDecreeNo,
                Decree = decree
            };

            return result;
        }

        public async Task<ProposeResult> BeginNewPropose(PaxosDecree decree, ulong decreeNo)
        {
            if (decreeNo == 0)
            {
                // other node may already proposed a same decreeNo
                // continue untill the decree proposed committed
                do
                {
                    var oneInstanceCompletionSource = new TaskCompletionSource<ProposeResult>();
                    await ProcessBeginNewBallotRequest(decree, decreeNo, oneInstanceCompletionSource);
                    var result = await oneInstanceCompletionSource.Task;
                    if (result.Decree.Content.Equals(decree.Content))
                    {
                        return result;
                    }
                } while (true);

            }
            else
            {
                var completionSource = new TaskCompletionSource<ProposeResult>();
                await ProcessBeginNewBallotRequest(decree, decreeNo, completionSource);
                var result = await completionSource.Task;
                return result;
            }
        }

        public Task<bool> DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            return ProcessStaleBallotMessage(msg);
        }

        public Task<bool> DeliverLastVoteMessage(LastVoteMessage msg)
        {
            return ProcessLastVoteMessage(msg);
        }

        public Task<bool> DeliverVoteMessage(VoteMessage msg)
        {
            return ProcessVoteMessage(msg);
        }

        private async Task ProcessBeginNewBallotRequest(
            PaxosDecree decree,
            ulong nextDecreeNo,
            TaskCompletionSource<ProposeResult> proposeCompletionNotification)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(nextDecreeNo);
            await decreeLock.AcquireLock();
            if (nextDecreeNo == 0)
            {
                //
                // several propose may happen concurrently. All of them will
                // get a unique decree no.
                //
                nextDecreeNo = _proposerNote.GetNewDecreeNo();
            }

            ulong ballotNo = 0;
            try
            {
                var committedDecree = await _proposerNote.GetCommittedDecree(nextDecreeNo);
                if (committedDecree != null)
                {
                    // already committed, return directly
                    var result = new ProposeResult()
                    {
                        DecreeNo = nextDecreeNo,
                        Decree = committedDecree
                    };

                    proposeCompletionNotification?.SetResult(result);
                    return;
                }

                _proposerNote.SubscribeCompletionNotification(nextDecreeNo, proposeCompletionNotification);

                ballotNo = _proposerNote.PrepareNewBallot(nextDecreeNo, decree);
                if (ballotNo == 0)
                {
                    // cant be, send alert
                }

            }
            finally
            {
                decreeLock.ReleaseLock();
            }

            QueryLastVote(nextDecreeNo, ballotNo);

            return;
        }

        private async Task<bool> ProcessStaleBallotMessage(StaleBallotMessage msg)
        {
            // QueryLastVote || BeginNewBallot
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            ulong nextBallotNo = 0;
            await decreeLock.AcquireLock();
            try
            {
                // in case committed, ongoing propose could be cleaned
                var propose = _proposerNote.GetPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }
                if (propose.State != PropserState.QueryLastVote && propose.State != PropserState.BeginNewBallot)
                {
                    return false;
                }

                // other stale message may already started new ballot, abandon this message
                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    return false;
                }

                // query last vote again
                var decree = propose.OngoingDecree;

                // can be optimized to get the right ballot no once
                // if concurrently stale message reach here, will git a diffent
                // ballo no, and one will become stale
                do
                {
                    nextBallotNo = _proposerNote.PrepareNewBallot(msg.DecreeNo, decree);
                } while (nextBallotNo <= msg.NextBallotNo);

            }
            finally
            {
                decreeLock.ReleaseLock();
            }


            // could be several different LastVote query on different ballot
            QueryLastVote(msg.DecreeNo, nextBallotNo);

            return true;
        }

        private async Task<bool> ProcessLastVoteMessage(LastVoteMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            await decreeLock.AcquireLock();

            try
            {
                // in case committed, ongoing propose could be cleaned
                var propose = _proposerNote.GetPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }

                //
                // 1. decree passed, and begin to vote the ballot
                // 2. decree committed
                //
                if (propose.State != PropserState.QueryLastVote)
                {
                    return false;
                }

                //
                // new ballot intialized, thsi lastvotemessage become stale message 
                //
                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    return false;
                }

                if (msg.Commited)
                {
                    // decree already committed
                    if (!_proposerNote.BeginCommit(msg.DecreeNo, msg.BallotNo, msg.VoteDecree))
                    {
                        return false;
                    }

                    await BeginCommit(msg.DecreeNo, msg.BallotNo);

                    return false;
                }

                // TODO: check if message come from existing node
                ulong lstVoteMsgCount = _proposerNote.AddLastVoteMessage(msg.DecreeNo, msg);
                if (lstVoteMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
                {
                    // enough feedback got
                    var newBallotDecree = _proposerNote.BeginNewBallot(msg.DecreeNo, msg.BallotNo);
                    if (newBallotDecree == null)
                    {
                        // sombody else is doing the job
                        return false;
                    }

                    // begin new ballot
                    BeginNewBallot(msg.DecreeNo, msg.BallotNo, newBallotDecree);
                }

                return true;
            }
            finally
            {
                decreeLock.ReleaseLock();
            }

        }

        private async Task<bool> ProcessVoteMessage(VoteMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            await decreeLock.AcquireLock();

            try
            {
                var propose = _proposerNote.GetPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }
                if (propose.State != PropserState.BeginNewBallot)
                {
                    return false;
                }

                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    // not the vote ballot
                    return false;
                }

                // TODO: check if message come from existing node
                var votesMsgCount = _proposerNote.AddVoteMessage(msg.DecreeNo, msg);

                if (votesMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
                {
                    // enough feedback got, begin to commit
                    if (!_proposerNote.BeginCommit(msg.DecreeNo, msg.BallotNo, msg.VoteDecree))
                    {
                        return false;
                    }

                    await BeginCommit(msg.DecreeNo, msg.BallotNo);
                }
                return true;
            }
            finally
            {
                decreeLock.ReleaseLock();
            }
        }


        private void QueryLastVote(UInt64 decreeNo, ulong nextBallotNo)
        {
            // 1. collect decree for this instance, send NextBallotMessage
            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var nextBallotMessage = new NextBallotMessage();
                nextBallotMessage.TargetNode = node.Name;
                nextBallotMessage.DecreeNo = decreeNo;
                nextBallotMessage.BallotNo = nextBallotNo;

                _messageTransport.SendMessage(nextBallotMessage);
            }

        }
        private void BeginNewBallot(UInt64 decreeNo, UInt64 ballotNo, PaxosDecree newBallotDecree)
        {
            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var beginBallotMessage = new BeginBallotMessage();
                beginBallotMessage.DecreeNo = decreeNo;
                beginBallotMessage.BallotNo = ballotNo;
                beginBallotMessage.TargetNode = node.Name;
                beginBallotMessage.Decree = newBallotDecree;
                _messageTransport.SendMessage(beginBallotMessage);
            }
        }

        private async Task BeginCommit(ulong decreeNo, ulong ballotNo)
        {
            // write the decree to ledge
            var propose = await _proposerNote.Commit(decreeNo);


            var subscriberList = propose.CompletionEvents;
            //_proposerNote.ClearDecree(decreeNo);

            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var successMessage = new SuccessMessage();
                successMessage.TargetNode = node.Name;
                successMessage.DecreeNo = decreeNo;
                successMessage.BallotNo = ballotNo;
                successMessage.Decree = propose.OngoingDecree;
                await _messageTransport.SendMessage(successMessage);
            }

            // TODO: confirm the commit succeeded on other nodes
            var result = new ProposeResult()
            {
                DecreeNo = decreeNo,
                Decree = propose.OngoingDecree
            };
            foreach (var completionSource in subscriberList)
            {
                completionSource.SetResult(result);
            }

        }
    }

}
