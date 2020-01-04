using Paxos.Common;
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

    public interface IPaxosNotification
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

    /// <summary>
    /// Event Received          Action
    /// LastVote                Return last vote if the new ballot no bigger than NextBallotNo recorded
    /// BeginNewBallot          If the new ballotNo is equal to NextBallotNo recorded,vote for the ballot
    ///                         and save the vote as last vote
    /// CommitDecree            Save the decree
    /// </summary>
    ///
    public class VoterRole : IDisposable
    {
        private readonly RpcClient _rpcClient;

        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;
        private readonly VoterNote _note;
        private readonly ProposerNote _ledger;
        IPaxosNotification _notificationSubscriber;

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            VoterNote voterNote,
            ProposerNote ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (voterNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _rpcClient = rpcClient;
            _note = voterNote;
            _ledger = ledger;
        }

        public virtual void Dispose()
        {

        }

        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
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
            // commit in logs
            await _ledger.CommitDecree(msg.DecreeNo, msg.Decree);

            if (_notificationSubscriber != null)
                await _notificationSubscriber.UpdateSuccessfullDecree(msg.DecreeNo, msg.Decree);
        }

        private async Task<PaxosMessage> ProcessNextBallotMessageInternal(NextBallotMessage msg)
        {
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
                var oldNextBallotNo = result.Item1;
                var lastVote = result.Item2;
                if (oldNextBallotNo >= msg.BallotNo)
                {
                    // do not response the ballotNo < current nextBallotNo
                    return new StaleBallotMessage()
                    {
                        NextBallotNo = oldNextBallotNo,
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
                    VoteBallotNo = lastVote != null ? lastVote.VotedBallotNo : 0,
                    VoteDecree = lastVote?.VotedDecree
                };
            }
        }

        private async Task<PaxosMessage> ProcessNewBallotMessageInternal(BeginBallotMessage msg)
        {
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
                    BallotNo = msg.BallotNo
                };

                // save last vote
                oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, msg.Decree);

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

    public class ProposerRole : IDisposable
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
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly ProposeManager _proposeManager;

        IPaxosNotification _notificationSubscriber;

        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            ProposerNote proposerNote,
            ProposeManager proposerManager)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _rpcClient = rpcClient;
            _proposerNote = proposerNote;
            _proposeManager = proposerManager;
            Stop = false;
        }

        public virtual void Dispose()
        { }

        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
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

            do
            {
                // 1. get decree no
                if (decreeNo == 0)
                {
                    //
                    // several propose may happen concurrently. All of them will
                    // get a unique decree no.
                    //
                    nextDecreeNo = _proposeManager.GetNextDecreeNo();
                    _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
                }

                var lastVoteResult = await CollectLastVote(decree, nextDecreeNo);
                if (lastVoteResult.IsCommitted)
                {
                    _proposeManager.RemovePropose(nextDecreeNo);

                    // already committed
                    if (decreeNo == 0)
                    {
                        continue;
                    }

                    return new ProposeResult()
                    {
                        Decree = lastVoteResult.CommittedDecree,
                        DecreeNo = lastVoteResult.DecreeNo
                    };
                }

                var propose = lastVoteResult.OngoingPropose;
                var nextBallotNo = propose.GetNextBallot();
                // check if stale message received
                var nextAction = await propose.GetNextAction();
                if (nextAction == Protocol.Propose.NextAction.CollectLastVote)
                {
                    continue;
                }
                else if (nextAction == Protocol.Propose.NextAction.Commit)
                {
                    await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);

                    if (decreeNo == 0)
                    {
                        continue;
                    }

                    return new ProposeResult()
                    {
                        Decree = propose.GetCommittedDecree(),
                        DecreeNo = lastVoteResult.DecreeNo
                    };
                }

                // begin new ballot
                var newBallotResult = await BeginNewBallot(lastVoteResult.DecreeNo, nextBallotNo);
                propose = newBallotResult.OngoingPropose;
                nextBallotNo = propose.GetNextBallot();
                nextAction = await propose.GetNextAction();
                if (nextAction == Protocol.Propose.NextAction.CollectLastVote)
                {
                    continue;
                }
                else if (nextAction == Protocol.Propose.NextAction.Commit)
                {
                    await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);
                    return new ProposeResult()
                    {
                        Decree = propose.GetCommittedDecree(),
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
            if (result.OngoingPropose.State == ProposeState.QueryLastVote ||
                result.OngoingPropose.State == ProposeState.BeginNewBallot)
            {
                var lastVoteResult = new ProposePhaseResult()
                { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };
                result.OngoingPropose?.Result?.SetResult(lastVoteResult);
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
                case LastVoteMessageResult.ResultAction.NewBallotReadyToBegin:
                    {
                        var phaseResult = new ProposePhaseResult()
                        {
                            DecreeNo = msg.DecreeNo,
                            OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo),
                            IsCommitted = LastVoteMessageResult.ResultAction.DecreeCommitted == result.Action
                        };

                        if (result.CommittedPropose.Result != null && !result.CommittedPropose.Result.Task.IsCompleted)
                            result.CommittedPropose.Result.SetResult(phaseResult);
                    }
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

                        var newBallotResult = new ProposePhaseResult()
                        { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };

                        if (propose.Result != null && !propose.Result.Task.IsCompleted)
                            propose.Result.SetResult(newBallotResult);

                    }
                    break;
            }
            return true;
        }

        public async Task<ProposePhaseResult> CollectLastVote(
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
            var propose = _proposeManager.GetOngoingPropose(nextDecreeNo);
            if (propose == null)
            {
                propose = _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
            }

            ulong ballotNo = 0;
            var completionSource = new TaskCompletionSource<ProposePhaseResult>();

            using (var autoLock = await propose.AcquireLock())
            {
                var proposeResult = await GetProposeResult(nextDecreeNo);
                if (proposeResult != null)
                {
                    // already committed, return directly
                    return new ProposePhaseResult()
                    {
                        DecreeNo = nextDecreeNo,
                        OngoingPropose = propose,
                        IsCommitted = true,
                        CommittedDecree = proposeResult.Decree
                    };
                }

                ballotNo = propose.PrepareNewBallot(decree);

                propose.Result = completionSource;

                if (ballotNo == 0)
                {
                    // cant be, send alert
                }
            }

            await BroadcastQueryLastVote(nextDecreeNo, ballotNo);

            return await completionSource.Task;
        }

        public async Task<ProposePhaseResult> BeginNewBallot(ulong decreeNo, ulong ballotNo)
        {
            PaxosDecree newBallotDecree = null;
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            if (propose == null)
            {
                // send alert
                return new ProposePhaseResult()
                {
                    DecreeNo = decreeNo,
                    OngoingPropose = null,
                    IsCommitted = false,
                    CommittedDecree = null
                };
            }

            var completionSource = new TaskCompletionSource<ProposePhaseResult>();
            using (var autoLock = await propose.AcquireLock())
            {

                newBallotDecree = propose.BeginNewBallot(ballotNo);
                if (newBallotDecree == null)
                {
                    // sombody else is doing the job, this could not happend since the decree lock is locked
                    return new ProposePhaseResult()
                    {
                        DecreeNo = decreeNo,
                        OngoingPropose = null,
                        IsCommitted = false,
                        CommittedDecree = null
                    };
                }

                propose.Result = completionSource;

            }

            await BroadcastBeginNewBallot(decreeNo, ballotNo, newBallotDecree);

            return await completionSource.Task;
        }

        private async Task<Propose> CommitPropose(ulong decreeNo, ulong ballotNo)
        {
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            using (var autoLock = await propose.AcquireLock())
            {
                if (!propose.Commit(ballotNo))
                {
                    // others may commit a new one with a different ballotNo
                    return null;
                }

                await _proposerNote.CommitDecree(decreeNo, propose.GetCommittedDecree());
            }

            await NotifyLearnersResult(decreeNo, ballotNo, propose.GetCommittedDecree());
            _proposeManager.RemovePropose(decreeNo);

            if (_notificationSubscriber != null)
                await _notificationSubscriber?.UpdateSuccessfullDecree(decreeNo, propose.GetCommittedDecree());

            return propose;
        }

        private async Task<StateBallotMessageResult> ProcessStaleBallotMessageInternal(StaleBallotMessage msg)
        {
            // in case committed, ongoing propose could be cleaned
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                return new StateBallotMessageResult()
                { NeedToCollectLastVote = false };
            }

            // QueryLastVote || BeginNewBallot
            ulong nextBallotNo = 0;
            using (var autoLock = await propose.AcquireLock())
            {
                if (propose.State != ProposeState.QueryLastVote && propose.State != ProposeState.BeginNewBallot)
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


                return new StateBallotMessageResult()
                { NeedToCollectLastVote = true, NextBallotNo = nextBallotNo, OngoingPropose = propose };
            }

        }

        private async Task<LastVoteMessageResult> ProcessLastVoteMessageInternal(LastVoteMessage msg)
        {
            // in case committed, ongoing propose could be cleaned
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                return new LastVoteMessageResult()
                { Action = LastVoteMessageResult.ResultAction.None };
            }

            using (var autoLock = await propose.AcquireLock())
            {

                //
                // 1. decree passed, and begin to vote the ballot
                // 2. decree committed
                //
                if (propose.State != ProposeState.QueryLastVote &&
                    propose.State != ProposeState.BeginNewBallot)
                {
                    return new LastVoteMessageResult()
                    { Action = LastVoteMessageResult.ResultAction.None };
                }

                if (propose.State == ProposeState.BeginNewBallot && !msg.Commited)
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
                    if (propose.Commit(msg.BallotNo))
                    {
                        var committedDecree = propose.GetCommittedDecree();
                        await _proposerNote.CommitDecree(msg.DecreeNo, committedDecree);
                    }
                    else
                    {
                        // somebody else already committed it
                    }

                    // decree already committed
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
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                // propose may already be committed
                return new VoteMessageResult()
                { Action = VoteMessageResult.ResultAction.None };
            }
            using (var autoLock = await propose.AcquireLock())
            {
                if (propose.State != ProposeState.BeginNewBallot)
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
            var tasks = new List<Task>();
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

                tasks.Add(SendPaxosMessage(nextBallotMessage));
            }
            await Task.WhenAll(tasks);
        }
        private async Task BroadcastBeginNewBallot(UInt64 decreeNo, UInt64 ballotNo, PaxosDecree newBallotDecree)
        {
            var tasks = new List<Task>();
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

                tasks.Add(SendPaxosMessage(beginBallotMessage));
            }
            await Task.WhenAll(tasks);
        }

        private async Task NotifyLearnersResult(ulong decreeNo, ulong ballotNo, PaxosDecree decree)
        {
            var tasks = new List<Task>();

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

                tasks.Add(SendPaxosMessage(successMessage));
            }
            await Task.WhenAll(tasks);
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
