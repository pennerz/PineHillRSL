using Paxos.Network;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Request;
using Paxos.Rpc;
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

    public class DecreeLockWrapper : IDisposable
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

        public async Task<AutoDecreeLock> AcquireLock()
        {
            await _decreeLock.AcquireLock();
            AutoDecreeLock autoLock = null;
            try
            {
                autoLock = new AutoDecreeLock(this);
            }
            finally
            {
                if (autoLock == null)
                {
                    _decreeLock.ReleaseLock();
                }
            }
            return autoLock;
        }

        public void ReleaseLock()
        {
            _decreeLock.ReleaseLock();
        }

        public void Dispose()
        {
            _decreeLock.ReferenceCount--;
        }
    }

    public class AutoDecreeLock : IDisposable
    {
        private DecreeLockWrapper _lock;
        public AutoDecreeLock(DecreeLockWrapper decreeLock)
        {
            _lock = decreeLock;
        }

        public void Dispose()
        {
            if (_lock != null)
            {
                _lock.ReleaseLock();
                _lock.Dispose();
            }
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
        private readonly RpcClient _rpcClient;
        private readonly DecreeLockManager _decreeLockManager;

        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;
        private readonly VoterNote _note;
        private readonly ProposerNote _ledger;

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            DecreeLockManager decreeLockManager,
            VoterNote voterNote,
            ProposerNote ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (voterNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");
            if (decreeLockManager == null) throw new ArgumentNullException("decree lock manager");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _rpcClient = rpcClient;
            _note = voterNote;
            _ledger = ledger;
            _decreeLockManager = decreeLockManager;
        }

        public async Task DeliverNextBallotMessage(NextBallotMessage msg)
        {
            // process nextballotmessage
            PaxosMessage respondPaxosMessage = await ProcessNextBallotMessageInternal(msg);

            // send a response message to proposer
            await SendPaxosMessage(respondPaxosMessage);
        }

        public async Task DeliverBeginBallotMessage(BeginBallotMessage msg)
        {
            // process newballotmessage
            var responsePaxosMsg = await ProcessNewBallotMessageInternal(msg);

            // send the response message back to proposer
            await SendPaxosMessage(responsePaxosMsg);
        }

        public async Task DeliverSuccessMessage(SuccessMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            using (var autolock = await decreeLock.AcquireLock())
            {
                // commit in logs
                await _ledger.CommitDecree(msg.DecreeNo, msg.Decree);
            }
        }

        private async Task<PaxosMessage> ProcessNextBallotMessageInternal(NextBallotMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            using (var autolock = await decreeLock.AcquireLock())
            {
                // check if committed
                var commitedDecree = await _ledger.GetCommittedDecree(msg.DecreeNo);
                if (commitedDecree != null)
                {
                    return new LastVoteMessage()
                    {
                        TargetNode = msg.SourceNode,
                        Commited = true,
                        BallotNo = msg.BallotNo,
                        DecreeNo = msg.DecreeNo,
                        VoteBallotNo = 0, // not applicable
                        VoteDecree = commitedDecree,
                        CommittedDecrees = _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                    };
                }

                // udpate next ballot no
                var result = await _note.UpdateNextBallotNo(msg.DecreeNo, msg.BallotNo);
                var oldBallotNo = result.Item1;
                var lastVote = result.Item2;
                if (oldBallotNo >= msg.BallotNo)
                {
                    // do not response the ballotNo < current nextBallotNo
                    return new StaleBallotMessage()
                    {
                        NextBallotNo = oldBallotNo,
                        TargetNode = msg.SourceNode,
                        BallotNo = msg.BallotNo,
                        DecreeNo = msg.DecreeNo
                    };
                }

                // it could be possible when this lastvotemessage is sent out
                // a new ballot has been voted, in that case, when this LastVoteMessage
                // returned, two case
                // 1. the new ballot is triggered by same node as this one, in this case
                //    this LastVoteMessage will be abandoned
                // 2. the new ballot is triggered by other node, in this case, the node
                //    trigger this ballot will accept this last LastVoteMessage, and it

                // send back the last vote information
                return new LastVoteMessage()
                {
                    TargetNode = msg.SourceNode,
                    BallotNo = msg.BallotNo,
                    DecreeNo = msg.DecreeNo,
                    VoteBallotNo = lastVote != null ? lastVote.BallotNo : 0,
                    VoteDecree = lastVote?.VoteDecree
                };
            }
        }

        private async Task<PaxosMessage> ProcessNewBallotMessageInternal(BeginBallotMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);

            using (var autolock = await decreeLock.AcquireLock())
            {
                // check if committed
                var commitedDecree = await _ledger.GetCommittedDecree(msg.DecreeNo);
                if (commitedDecree != null)
                {
                    // TODO send back a message to accelerate the process
                    return new LastVoteMessage()
                    {
                        TargetNode = msg.SourceNode,
                        DecreeNo = msg.DecreeNo,
                        BallotNo = msg.BallotNo,
                        Commited = true,
                        VoteDecree = commitedDecree,
                        CommittedDecrees = _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                    };
                }

                var oldNextBallotNo = _note.GetNextBallotNo(msg.DecreeNo);
                if (msg.BallotNo > oldNextBallotNo)
                {
                    // should not happend, send alert
                    return null;
                }

                if (msg.BallotNo < oldNextBallotNo)
                {
                    // stale message
                    return new StaleBallotMessage()
                    {
                        TargetNode = msg.SourceNode,
                        DecreeNo = msg.DecreeNo,
                        BallotNo = msg.BallotNo,
                        NextBallotNo = oldNextBallotNo
                    };
                }

                // vote this ballot
                var voteMsg = new VoteMessage()
                {
                    TargetNode = msg.SourceNode,
                    DecreeNo = msg.DecreeNo,
                    BallotNo = msg.BallotNo,
                    VoteDecree = msg.Decree
                };

                // save last vote
                oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, voteMsg);

                return voteMsg;
            }
        }

        private async Task SendPaxosMessage(PaxosMessage paxosMessage)
        {
            if (paxosMessage == null)
            {
                return;
            }
            paxosMessage.SourceNode = _nodeInfo.Name;
            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMessage);
            var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
            var remoteAddr = new NodeAddress()
            { Node = new NodeInfo() { Name = paxosMessage.TargetNode }, Port = 88};

            await _rpcClient.SendRequest(remoteAddr, rpcMsg);
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
        private class LastVoteMessageResult
        {
            public enum ResultAction { None, DecreeCommitted, NewBallotReadyToBegin };
            public ResultAction Action { get; set; }
            public PaxosDecree NewBallotDecree { get; set; }
            public ulong NextBallotNo { get; set; }
            public Propose CommittedPropose { get; set; }
        }

        private class StateBallotMessageResult
        {
            public bool NeedToCollectLastVote { get; set; }
            public ulong NextBallotNo { get; set; }
            public Propose OngoingPropose { get; set; }
        }

        private class VoteMessageResult
        {
            public enum ResultAction { None, ReadyToCommit };
            public ResultAction Action { get; set; }
        };


        private readonly RpcClient _rpcClient;
        private readonly DecreeLockManager _decreeLockManager;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly ProposeManager _proposeManager;

        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            DecreeLockManager decreeLockManager,
            ProposerNote proposerNote,
            ProposeManager proposerManager)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");
            if (decreeLockManager == null) throw new ArgumentNullException("decreeLock manager");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _rpcClient = rpcClient;
            _proposerNote = proposerNote;
            _decreeLockManager = decreeLockManager;
            _proposeManager = proposerManager;
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

            // committed decree can never be changed, no need to lock the decree no
            var decree = await _proposerNote.GetCommittedDecree(decreeNo);
            result = new DecreeReadResult()
            {
                IsFound = true,
                MaxDecreeNo = maximumCommittedDecreeNo,
                Decree = decree
            };

            return result;
        }

        public async Task<ProposeResult> Propose(PaxosDecree decree, ulong decreeNo)
        {

            ulong nextDecreeNo = decreeNo;
            // 1. get decree no
            if (nextDecreeNo == 0)
            {
                //
                // several propose may happen concurrently. All of them will
                // get a unique decree no.
                //
                nextDecreeNo = _proposeManager.GetNextDecreeNo();
                _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
            }


            do
            {
                var lastVoteResult = await CollectLastVote(decree, nextDecreeNo);
                if (lastVoteResult.IsCommitted)
                {
                    // already committed
                    if (decreeNo == 0)
                    {
                        nextDecreeNo = _proposeManager.GetNextDecreeNo();
                        _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
                        continue;
                    }

                    return new ProposeResult()
                    {
                        Decree = lastVoteResult.CommittedDecree,
                        DecreeNo = lastVoteResult.DecreeNo
                    };
                }

                // check if stale message received
                var propose = lastVoteResult.OngoingPropose;
                var nextBallotNo = propose.GetNextBallot();
                var nextAction = await propose.GetNextAction();
                if (nextAction == Notebook.Propose.NextAction.CollectLastVote)
                {
                    continue;
                }
                else if (nextAction == Notebook.Propose.NextAction.Commit)
                {
                    var commitDecree = propose.GetCommittedDecree();
                    propose.OngoingDecree = commitDecree;
                    await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);
                    await NotifyLearnersResult(lastVoteResult.DecreeNo, nextBallotNo, commitDecree);
                    if (decreeNo == 0)
                    {
                        continue;
                    }
                    return new ProposeResult()
                    {
                        Decree = commitDecree,
                        DecreeNo = lastVoteResult.DecreeNo
                    };
                }

                // begin new ballot
                var newBallotResult = await BeginNewBallot(lastVoteResult.DecreeNo, nextBallotNo);
                propose = newBallotResult.OngoingPropose;
                nextBallotNo = propose.GetNextBallot();
                nextAction = await propose.GetNextAction();
                if (nextAction == Notebook.Propose.NextAction.CollectLastVote)
                {
                    continue;
                }
                else if (nextAction == Notebook.Propose.NextAction.Commit)
                {
                    var commitDecree = propose.GetCommittedDecree();
                    propose.OngoingDecree = commitDecree;
                    await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);
                    await NotifyLearnersResult(lastVoteResult.DecreeNo, nextBallotNo, commitDecree);

                    return new ProposeResult()
                    {
                        Decree = commitDecree,
                        DecreeNo = lastVoteResult.DecreeNo
                    };
                }
            } while (true);
        }

        public async Task<bool> DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            var result = await ProcessStaleBallotMessageInternal(msg);
            if (result.NeedToCollectLastVote == false) //  no need to query last vote again
            {
                return false;
            }
            if (result.OngoingPropose.State == PropserState.QueryLastVote)
            {
                var lastVoteResult = new LastVoteCollectResult()
                { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };
                result.OngoingPropose?.LastVoteResult?.SetResult(lastVoteResult);
            }
            else if(result.OngoingPropose.State == PropserState.BeginNewBallot)
            {
                var ballotResult = new BallotResult()
                { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };
                result.OngoingPropose?.NewBallotResult?.SetResult(ballotResult);

            }
            //await BroadcastQueryLastVote(msg.DecreeNo, result.NextBallotNo);
            return true;
        }

        public async Task<bool> DeliverLastVoteMessage(LastVoteMessage msg)
        {
            var result = await ProcessLastVoteMessageInternal(msg);
            switch (result.Action)
            {
                case LastVoteMessageResult.ResultAction.None:
                    break;
                case LastVoteMessageResult.ResultAction.DecreeCommitted:
                    {
                        var lastVoteResult = new LastVoteCollectResult()
                        { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };

                        if (result.CommittedPropose.LastVoteResult != null && !result.CommittedPropose.LastVoteResult.Task.IsCompleted)
                            result.CommittedPropose.LastVoteResult.SetResult(lastVoteResult);

                    }
                    //await NotifyLearnersResult(msg.DecreeNo, msg.BallotNo, result.CommittedPropose?.OngoingDecree);
                    //await NotifyCompletion(msg.DecreeNo, result.CommittedPropose);
                    break;
                case LastVoteMessageResult.ResultAction.NewBallotReadyToBegin:
                    {
                        var lastVoteResult = new LastVoteCollectResult()
                        { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };

                        if (result.CommittedPropose.LastVoteResult != null && !result.CommittedPropose.LastVoteResult.Task.IsCompleted)
                        {
                            result.CommittedPropose.LastVoteResult.SetResult(lastVoteResult);
                        }

                    }
                    //await BeginNewBallot(msg.DecreeNo, msg.BallotNo);
                    break;
            }

            return true;
        }

        public async Task<bool> DeliverVoteMessage(VoteMessage msg)
        {
            var result = await ProcessVoteMessageInternal(msg);
            switch (result.Action)
            {
                case VoteMessageResult.ResultAction.ReadyToCommit:
                    {
                        var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                        if (propose == null)
                        {
                            return false;
                        }

                        var newBallotResult = new BallotResult()
                        { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };

                        if (propose.NewBallotResult != null && !propose.NewBallotResult.Task.IsCompleted)
                            propose.NewBallotResult.SetResult(newBallotResult);

                    }
                    //var committedPropose = await CommitPropose(msg.DecreeNo, msg.BallotNo);
                    //await NotifyLearnersResult(msg.DecreeNo, msg.BallotNo, committedPropose.OngoingDecree);
                    //await NotifyCompletion(msg.DecreeNo, committedPropose);
                    break;
            }
            return true;
        }


        public async Task<LastVoteCollectResult> CollectLastVote(
            PaxosDecree decree,
            ulong nextDecreeNo)
        {
            // 1. get decree no
            if (nextDecreeNo == 0)
            {
                //
                // several propose may happen concurrently. All of them will
                // get a unique decree no.
                //
                nextDecreeNo = _proposeManager.GetNextDecreeNo();
                _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
            }

            ulong ballotNo = 0;
            var completionSource = new TaskCompletionSource<LastVoteCollectResult>();

            var decreeLock = _decreeLockManager.GetDecreeLock(nextDecreeNo);
            using (var autoLock = await decreeLock.AcquireLock())
            {

                var proposeResult = await GetProposeResult(nextDecreeNo);
                if (proposeResult != null)
                {
                    // already committed, return directly
                    return new LastVoteCollectResult()
                    {
                        DecreeNo = nextDecreeNo,
                        OngoingPropose = null,
                        IsCommitted = true,
                        CommittedDecree = proposeResult.Decree
                    };
                }

                var propose = _proposeManager.GetOngoingPropose(nextDecreeNo);
                if (propose == null)
                {
                    propose = _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
                }

                ballotNo = propose.PrepareNewBallot(decree);

                propose.LastVoteResult = completionSource;

                if (ballotNo == 0)
                {
                    // cant be, send alert
                }
            }

            await BroadcastQueryLastVote(nextDecreeNo, ballotNo);

            return await completionSource.Task;
        }

        public async Task<BallotResult> BeginNewBallot(ulong decreeNo, ulong ballotNo)
        {
            PaxosDecree newBallotDecree = null;
            var completionSource = new TaskCompletionSource<BallotResult>();
            var decreeLock = _decreeLockManager.GetDecreeLock(decreeNo);
            using (var autoLock = await decreeLock.AcquireLock())
            {

                var propose = _proposeManager.GetOngoingPropose(decreeNo);
                if (propose == null)
                {
                    // send alert
                    return new BallotResult()
                    {
                       DecreeNo = decreeNo,
                       OngoingPropose = null,
                       IsCommitted = false,
                       CommittedDecree = null
                    };
                }
                newBallotDecree = propose.BeginNewBallot(ballotNo);
                if (newBallotDecree == null)
                {
                    // sombody else is doing the job, this could not happend since the decree lock is locked
                    return new BallotResult()
                    {
                        DecreeNo = decreeNo,
                        OngoingPropose = null,
                        IsCommitted = false,
                        CommittedDecree = null
                    };
                }

                propose.NewBallotResult = completionSource;

            }

            await BroadcastBeginNewBallot(decreeNo, ballotNo, newBallotDecree);

            return await completionSource.Task;
        }

        public async Task<Propose> CommitPropose(ulong decreeNo, ulong ballotNo)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(decreeNo);
            using (var autoLock = await decreeLock.AcquireLock())
            {

                var propose = _proposeManager.GetOngoingPropose(decreeNo);
                if (propose == null)
                {
                    // send alert
                    return null;
                }

                if (!propose.BeginCommit(ballotNo, propose.OngoingDecree))
                {
                    // others may commit a new one with a different ballotNo
                    return null;
                }

                await _proposerNote.CommitDecree(decreeNo, propose.OngoingDecree);
                propose.State = PropserState.Commited;
                _proposeManager.RemovePropose(decreeNo);

                return propose; // committed propose
            }
        }

        private async Task<StateBallotMessageResult> ProcessStaleBallotMessageInternal(StaleBallotMessage msg)
        {
            // QueryLastVote || BeginNewBallot
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            ulong nextBallotNo = 0;
            using (var autoLock = await decreeLock.AcquireLock())
            {
                // in case committed, ongoing propose could be cleaned
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return new StateBallotMessageResult()
                    { NeedToCollectLastVote = false };
                }
                if (propose.State != PropserState.QueryLastVote && propose.State != PropserState.BeginNewBallot)
                {
                    return new StateBallotMessageResult()
                    { NeedToCollectLastVote = false };
                }

                // other stale message may already started new ballot, abandon this message
                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    return new StateBallotMessageResult()
                    { NeedToCollectLastVote = false };
                }

                propose.AddStaleBallotMessage(msg);

                // query last vote again
                var decree = propose.OngoingDecree;

                // can be optimized to get the right ballot no once
                // if concurrently stale message reach here, will git a diffent
                // ballo no, and one will become stale
                var oldState = propose.State;
                do
                {
                    nextBallotNo = propose.PrepareNewBallot(decree);
                } while (nextBallotNo <= msg.NextBallotNo);
                propose.State = oldState;
                propose.LastVoteMessages.Clear();
                return new StateBallotMessageResult()
                { NeedToCollectLastVote = true, NextBallotNo = nextBallotNo, OngoingPropose = propose };
            }

        }

        private async Task<LastVoteMessageResult> ProcessLastVoteMessageInternal(LastVoteMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);

            using (var autoLock = await decreeLock.AcquireLock())
            {
                // in case committed, ongoing propose could be cleaned
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None};
                }

                //
                // 1. decree passed, and begin to vote the ballot
                // 2. decree committed
                //
                if (propose.State != PropserState.QueryLastVote &&
                    propose.State != PropserState.BeginNewBallot)
                {
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None };
                }

                if (propose.State == PropserState.BeginNewBallot && !msg.Commited)
                {
                    // voter may find the decree committed by others
                    // it will responed this beginnewballot with a
                    // lastvote mssage, indicate the decree committed
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None };
                }

                //
                // new ballot intialized, this lastvotemessage become stale message 
                //
                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None };
                }

                ulong lstVoteMsgCount = propose.AddLastVoteMessage(msg);
                if (msg.Commited)
                {
                    // decree already committed
                    propose.OngoingDecree = msg.VoteDecree;
                    return new LastVoteMessageResult()
                    {
                        Action = LastVoteMessageResult.ResultAction.DecreeCommitted,
                        NextBallotNo = msg.BallotNo,
                        CommittedPropose = propose
                    };
                }

                // TODO: check if message come from existing node
                if (lstVoteMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
                {
                    // enough feedback got
                    return new LastVoteMessageResult()
                    {
                        Action = LastVoteMessageResult.ResultAction.NewBallotReadyToBegin,
                        CommittedPropose = propose,
                        NextBallotNo = msg.BallotNo
                    };
                }
            }

            return new LastVoteMessageResult()
            {Action = LastVoteMessageResult.ResultAction.None};
        }

        private async Task<VoteMessageResult> ProcessVoteMessageInternal(VoteMessage msg)
        {
            var decreeLock = _decreeLockManager.GetDecreeLock(msg.DecreeNo);
            using (var autoLock = await decreeLock.AcquireLock())
            {
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    // propose may already be committed
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.None };
                }
                if (propose.State != PropserState.BeginNewBallot)
                {
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.None };
                }

                if (propose.LastTriedBallot != msg.BallotNo)
                {
                    // not the vote ballot
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.None };
                }

                // TODO: check if message come from existing node
                var votesMsgCount = propose.AddVoteMessage(msg);

                if (votesMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
                {
                    // enough feedback got, begin to commit
                    return new VoteMessageResult()
                    { Action = VoteMessageResult.ResultAction.ReadyToCommit };
                }
            }
            return new VoteMessageResult()
            { Action = VoteMessageResult.ResultAction.None };
        }

        private async Task<ProposeResult> GetProposeResult(ulong decreeNo)
        {
            var committedDecree = await _proposerNote.GetCommittedDecree(decreeNo);
            if (committedDecree != null)
            {
                // already committed, return directly
                return new ProposeResult()
                {
                    DecreeNo = decreeNo,
                    Decree = committedDecree
                };
            }

            return null;
        }

        private async Task BroadcastQueryLastVote(UInt64 decreeNo, ulong nextBallotNo)
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

                await SendPaxosMessage(nextBallotMessage);
            }

        }
        private async Task BroadcastBeginNewBallot(UInt64 decreeNo, UInt64 ballotNo, PaxosDecree newBallotDecree)
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

                await SendPaxosMessage(beginBallotMessage);
            }
        }

        private async Task NotifyLearnersResult(ulong decreeNo, ulong ballotNo, PaxosDecree decree)
        {
            var successfullMessageList = new List<SuccessMessage>();

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
                successMessage.Decree = decree;

                await SendPaxosMessage(successMessage);
            }
        }

        private Task NotifyCompletion(ulong decreeNo, Propose comittedPropose)
        {
            if (comittedPropose == null)
            {
                return Task.CompletedTask;
            }

            var subscriberList = comittedPropose.CompletionEvents;
            var result = new ProposeResult()
            {
                DecreeNo = decreeNo,
                Decree = comittedPropose.OngoingDecree
            };
            foreach (var completionSource in subscriberList)
            {
                completionSource.SetResult(result);
            }
            return Task.CompletedTask;
        }

        private async Task SendPaxosMessage(PaxosMessage paxosMessage)
        {
            paxosMessage.SourceNode = _nodeInfo.Name;
            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMessage);
            var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
            var remoteAddr = new NodeAddress()
            { Node = new NodeInfo() { Name = paxosMessage.TargetNode }, Port = 88 };
            await _rpcClient.SendRequest(remoteAddr, rpcMsg);
        }
    }

}
