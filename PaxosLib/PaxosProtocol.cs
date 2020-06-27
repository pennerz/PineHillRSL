using PineRSL.Common;
using PineRSL.Network;
using PineRSL.Paxos.Message;
using PineRSL.Paxos.Notebook;
using PineRSL.Paxos.Request;
using PineRSL.Paxos.Rpc;
using PineRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.IO;

namespace PineRSL.Paxos.Protocol
{

    public interface IPaxosNotification
    {
        Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree);
        Task<UInt64> Checkpoint(Stream checkpointStream);
        Task LoadCheckpoint(UInt64 decreeNo, Stream checkpointStream);
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

    public abstract class PaxosRole : IDisposable
    {
        private class PaxosMessageQeuue
        {
            private SemaphoreSlim _lock = new SemaphoreSlim(3);
            private readonly RpcClient _rpcClient;
            private readonly NodeInfo _nodeInfo;
            private List<PaxosMessage> _messageQueue = new List<PaxosMessage>();

            public PaxosMessageQeuue(NodeInfo nodeInfo, RpcClient rpcClient)
            {
                _nodeInfo = nodeInfo;
                _rpcClient = rpcClient;
            }

            public void AddPaxosMessage(PaxosMessage paxosMessage)
            {
                paxosMessage.SourceNode = _nodeInfo.Name;
                lock (_lock)
                {
                    _messageQueue.Add(paxosMessage);
                }

                if (_lock.CurrentCount > 0)
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await _lock.WaitAsync();
                            do
                            {
                                var paxosMsg = PopMessage();
                                if (paxosMsg == null)
                                {
                                    return;
                                }
                                paxosMsg.SourceNode = _nodeInfo.Name;
                                var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMsg);
                                var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                                rpcMsg.NeedResp = false;
                                var remoteAddr = new NodeAddress(new NodeInfo(paxosMessage.TargetNode), 88);

                                var task = _rpcClient.SendRequest(remoteAddr, rpcMsg);

                            } while (true);
                        }
                        finally
                        {
                            _lock.Release();
                        }
                    });

                }
            }

            public PaxosMessage PopMessage()
            {
                var msgQueue = new List<PaxosMessage>();
                List<PaxosMessage> consumedMessages = null;
                lock(_lock)
                {
                    consumedMessages = _messageQueue;
                    _messageQueue = msgQueue;
                }
                if (consumedMessages.Count == 1)
                {
                    return consumedMessages[0];
                }
                else if (consumedMessages.Count == 0)
                {
                    return null;
                }
                else
                {
                    var aggregatedMsg = new AggregatedPaxosMessage();
                    aggregatedMsg.TargetNode = consumedMessages[0].TargetNode;
                    foreach (var msg in consumedMessages)
                    {
                        aggregatedMsg.AddPaxosMessage(msg);
                    }
                    return aggregatedMsg;
                }
            }

            public int MessageCount => _messageQueue.Count;
        }

        private readonly RpcClient _rpcClient;
        private readonly NodeInfo _nodeInfo;
        private ConcurrentDictionary<string, PaxosMessageQeuue> _paxosMessageList =
            new ConcurrentDictionary<string, PaxosMessageQeuue>();
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        private List<PaxosMessage> _procssingMsgList = new List<PaxosMessage>();

        public PaxosRole(
            NodeInfo nodeInfo,
            RpcClient rpcClient)
        {
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            Stop = false;
            _nodeInfo = nodeInfo;
            _rpcClient = rpcClient;
        }

        public void Dispose()
        {

        }

        public bool Stop { get; set; }
        public bool WaitRpcResp { get; set; }

        public NodeInfo Node => _nodeInfo;

        public async Task<bool> HandlePaxosMessage(PaxosMessage msg)
        {
            lock(_procssingMsgList)
            {
                if (Stop)
                {
                    return false;
                }
                _procssingMsgList.Add(msg);
            }

            try
            {
                bool ret = await DoDeliverPaxosMessage(msg);
                lock (_procssingMsgList)
                {
                    _procssingMsgList.Remove(msg);
                }
                return ret;
            }
            catch(Exception)
            {
                lock (_procssingMsgList)
                {
                    _procssingMsgList.Remove(msg);
                }
            }

            return false;
        }

        public async Task<PaxosMessage> HandleRequest(PaxosMessage request)
        {
            lock (_procssingMsgList)
            {
                if (Stop)
                {
                    return null;
                }
                _procssingMsgList.Add(request);
            }

            try
            {
                var ret = await DoRequest(request);
                lock (_procssingMsgList)
                {
                    _procssingMsgList.Remove(request);
                }
                return ret;
            }
            catch (Exception)
            {
                lock (_procssingMsgList)
                {
                    _procssingMsgList.Remove(request);
                }
            }
            return null;
        }


        protected abstract Task<bool> DoDeliverPaxosMessage(PaxosMessage msg);
        protected abstract Task<PaxosMessage> DoRequest(PaxosMessage request);


        protected async Task SendPaxosMessage(PaxosMessage paxosMessage)
        {
            if (paxosMessage == null)
            {
                return;
            }
            /*
            paxosMessage.SourceNode = _nodeInfo.Name;
            var remoteAddr = new NodeAddress(new NodeInfo(paxosMessage.TargetNode), 88);
            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMessage);
            var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
            rpcMsg.NeedResp = false;
            await _rpcClient.SendRequest(remoteAddr, rpcMsg);
            return;
            */

            var messageQueue = GetMessageQueue(paxosMessage.TargetNode);
            messageQueue.AddPaxosMessage(paxosMessage);
        }

        protected async Task<PaxosMessage> Request(PaxosMessage request)
        {
            request.SourceNode = _nodeInfo.Name;
            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(request);
            var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
            rpcMsg.NeedResp = true;
            var remoteAddr = new NodeAddress(new NodeInfo(request.TargetNode), 88);

            var rpcResp = await _rpcClient.SendRequest(remoteAddr, rpcMsg);
            if (rpcResp == null)
            {
                return null;
            }
            var paxosRpcMsgResp = PaxosRpcMessageFactory.CreatePaxosRpcMessage(rpcResp);
            var resp = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMsgResp);
            return resp;
        }

        public async Task WaitForAllMessageSent()
        {
            foreach(var messageList in _paxosMessageList)
            {
                while(messageList.Value.MessageCount > 0)
                {
                    await Task.Delay(100);
                }
            }
            await Task.Delay(100);
        }

        private PaxosMessageQeuue GetMessageQueue(string targetNode)
        {
            PaxosMessageQeuue messageQueue = null;
            do
            {
                if (_paxosMessageList.TryGetValue(targetNode, out messageQueue))
                {
                    return messageQueue;
                }
                messageQueue = new PaxosMessageQeuue(_nodeInfo, _rpcClient);
                _paxosMessageList.TryAdd(targetNode, messageQueue);
            } while (true);
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
    public class VoterRole : PaxosRole, IDisposable
    {
        private readonly NodeInfo _nodeInfo;
        private readonly VoterNote _note;
        private readonly ProposerNote _ledger;
        private IPaxosNotification _notificationSubscriber;
        private Task _truncateLogTask = null;
        private bool _exit = false;

        private List<PaxosMessage> _ongoingMessages = new List<PaxosMessage>();

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            VoterNote voterNote,
            ProposerNote ledger)
            : base(nodeInfo, rpcClient)
        {
            if (voterNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _note = voterNote;
            _ledger = ledger;

            WaitRpcResp = false;

            _truncateLogTask = Task.Run(async () =>
            {
                int truncateRound = 0;
                while(!_exit)
                {
                    await Task.Delay(1000);
                    if (truncateRound < 600)
                    {
                        truncateRound++;
                        continue;
                    }
                    truncateRound = 0;
                    var maxDecreeNo = _ledger.GetMaximumCommittedDecreeNo();
                    var position = _note.GetMaxPositionForDecrees(maxDecreeNo);
                    _note.Truncate(maxDecreeNo, position);
                }
            });
        }

        public new void Dispose()
        {
            _exit = true;
            Stop = true;
            base.Dispose();

            _truncateLogTask.Wait();

        }

        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
        }

        protected override async Task<bool> DoDeliverPaxosMessage(PaxosMessage msg)
        {
            switch(msg.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    await DeliverNextBallotMessage(msg as NextBallotMessage);
                    return true;
                case PaxosMessageType.BEGINBALLOT:
                    await DeliverBeginBallotMessage(msg as BeginBallotMessage);
                    return true;
                case PaxosMessageType.SUCCESS:
                    await DeliverSuccessMessage(msg as SuccessMessage);
                    return true;
                default:
                    break;
            }
            throw new NotImplementedException();
        }

        protected override Task<PaxosMessage> DoRequest(PaxosMessage request)
        {
            throw new NotImplementedException();
        }

        private async Task DeliverNextBallotMessage(NextBallotMessage msg)
        {
            lock(_ongoingMessages)
            {
                if (_exit)
                {
                    return;
                }

                _ongoingMessages.Add(msg);
            }
            // process nextballotmessage
            PaxosMessage respondPaxosMessage = await ProcessNextBallotMessageInternal(msg);

            // send a response message to proposer
            await SendPaxosMessage(respondPaxosMessage);

            lock(_ongoingMessages)
            {
                _ongoingMessages.Remove(msg);
            }
        }

        private async Task DeliverBeginBallotMessage(BeginBallotMessage msg)
        {
            lock(_ongoingMessages)
            {
                if (_exit)
                {
                    return;
                }
                _ongoingMessages.Add(msg);
            }
            // process newballotmessage
            var responsePaxosMsg = await ProcessNewBallotMessageInternal(msg);

            // send the response message back to proposer
            await SendPaxosMessage(responsePaxosMsg);

            lock(_ongoingMessages)
            {
                _ongoingMessages.Remove(msg);
            }
        }

        private async Task DeliverSuccessMessage(SuccessMessage msg)
        {
            lock(_ongoingMessages)
            {
                if (_exit)
                {
                    return;
                }
                _ongoingMessages.Add(msg);
            }

            // commit in logs
            //await _ledger.CommitDecree(msg.DecreeNo, new PaxosDecree(msg.Decree));

            //if (_notificationSubscriber != null)
            //    await _notificationSubscriber.UpdateSuccessfullDecree(msg.DecreeNo, new PaxosDecree(msg.Decree));

            // for test, control memory comsumtion
            _note.RemoveBallotInfo(msg.DecreeNo);

            lock(_ongoingMessages)
            {
                _ongoingMessages.Remove(msg);
            }
        }

        private async Task<PaxosMessage> ProcessNextBallotMessageInternal(NextBallotMessage msg)
        {
            {
                // check if committed
                var commitedDecreeInfo = await _ledger.GetCommittedDecree(msg.DecreeNo);
                if (commitedDecreeInfo != null)
                {
                    if (msg.DecreeNo < commitedDecreeInfo.CheckpointedDecreeNo)
                    {
                        // decree already checkpointed
                        return new LastVoteMessage()
                        {
                            TargetNode = msg.SourceNode,
                            Commited = true,
                            BallotNo = msg.BallotNo,
                            DecreeNo = msg.DecreeNo,
                            CheckpointedDecreNo = commitedDecreeInfo.CheckpointedDecreeNo,
                            VoteBallotNo = 0, // not applicable
                            VoteDecree = null,
                            CommittedDecrees = null
                        };
                    }
                    else
                    {
                        if (commitedDecreeInfo.CommittedDecree != null)
                        {
                            return new LastVoteMessage()
                            {
                                TargetNode = msg.SourceNode,
                                Commited = true,
                                BallotNo = msg.BallotNo,
                                DecreeNo = msg.DecreeNo,
                                CheckpointedDecreNo = commitedDecreeInfo.CheckpointedDecreeNo,
                                VoteBallotNo = 0, // not applicable
                                VoteDecree = commitedDecreeInfo.CommittedDecree.Data,
                                CommittedDecrees = await _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                            };
                        }
                    }
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
                    VoteDecree = lastVote?.VotedDecree.Data
                };
            }
        }

        private async Task<PaxosMessage> ProcessNewBallotMessageInternal(BeginBallotMessage msg)
        {
            {
                // check if committed
                var commitedDecreeInfo = await _ledger.GetCommittedDecree(msg.DecreeNo);
                if (commitedDecreeInfo != null)
                {
                    if (msg.DecreeNo < commitedDecreeInfo.CheckpointedDecreeNo)
                    {
                        // decree already checkpointed
                        return new LastVoteMessage()
                        {
                            TargetNode = msg.SourceNode,
                            Commited = true,
                            BallotNo = msg.BallotNo,
                            DecreeNo = msg.DecreeNo,
                            CheckpointedDecreNo = commitedDecreeInfo.CheckpointedDecreeNo,
                            VoteBallotNo = 0, // not applicable
                            VoteDecree = null,
                            CommittedDecrees = null
                        };
                    }
                    else
                    {
                        if (commitedDecreeInfo.CommittedDecree != null)
                        {
                            return new LastVoteMessage()
                            {
                                TargetNode = msg.SourceNode,
                                Commited = true,
                                BallotNo = msg.BallotNo,
                                DecreeNo = msg.DecreeNo,
                                CheckpointedDecreNo = commitedDecreeInfo.CheckpointedDecreeNo,
                                VoteBallotNo = 0, // not applicable
                                VoteDecree = commitedDecreeInfo.CommittedDecree.Data,
                                CommittedDecrees = await _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                            };
                        }
                    }
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
                oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, new PaxosDecree(msg.Decree));

                return voteMsg;
            }
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

    public class ProposerRole : PaxosRole, IDisposable
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


        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly ProposeManager _proposeManager;

        IPaxosNotification _notificationSubscriber;

        private List<PaxosMessage> _ongoingRequests = new List<PaxosMessage>();
        private int _catchupLogSize = 0;

        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            RpcClient rpcClient,
            ProposerNote proposerNote,
            ProposeManager proposerManager)
            : base(nodeInfo, rpcClient)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _proposerNote = proposerNote;
            _proposeManager = proposerManager;
            Stop = false;
            NotifyLearners = true;
        }

        public new void Dispose()
        {
            Stop = true;
            base.Dispose();
            while(true)
            {
                lock(_ongoingRequests)
                {
                    if (_ongoingRequests.Count == 0)
                    {
                        break;
                    }
                }
                Thread.Sleep(1000);
            }
        }

        public async Task Load()
        {
            await _proposerNote.Load();
            int catchupLogSize = 0;
            do
            {
                if (_notificationSubscriber != null)
                {
                    var metaRecord = _proposerNote.ProposeRoleMetaRecord;
                    if (metaRecord != null && !string.IsNullOrEmpty(metaRecord.CheckpointFilePath))
                    {
                        using (var checkpointStream = new FileStream(metaRecord.CheckpointFilePath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                        {
                            await _notificationSubscriber.LoadCheckpoint(metaRecord.DecreeNo, checkpointStream);
                        }
                    }

                    if (_proposerNote.CommittedDecrees.Count == 0)
                    {
                        PaxosDecree fakeDecree = new PaxosDecree();
                        await Propose(fakeDecree, 0);
                    }

                    // catchup 
                    foreach (var committedDecree in _proposerNote.CommittedDecrees)
                    {
                        await _notificationSubscriber.UpdateSuccessfullDecree(committedDecree.Key, committedDecree.Value);
                        if (committedDecree.Value.Data != null)
                        {
                            catchupLogSize += committedDecree.Value.Data.Length;
                        }
                    }
                }

                // request remote node's checkpoints
                bool needLoadNewCheckpoint = await LoadCheckpointFromRemoteNodes();
                if (!needLoadNewCheckpoint)
                {
                    break;
                }
                catchupLogSize = 0;
                // check if need to continue
            } while (true);
            _catchupLogSize = catchupLogSize;
        }

        private async Task<bool> LoadCheckpointFromRemoteNodes()
        {
            var summaryRequestTaskList = new List<Task<PaxosMessage>>();
            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var checkpointSummaryRequest = new CheckpointSummaryRequest();
                checkpointSummaryRequest.TargetNode = node.Name;
                var task = Request(checkpointSummaryRequest);
                summaryRequestTaskList.Add(task);
            }
            await Task.WhenAll(summaryRequestTaskList);
            CheckpointSummaryResp latestCheckpoint = null;
            foreach (var reqTask in summaryRequestTaskList)
            {
                var resp = reqTask.Result as CheckpointSummaryResp;
                if (resp == null)
                {
                    continue;
                }
                if (latestCheckpoint == null ||
                    resp.CheckpointDecreeNo > latestCheckpoint.CheckpointDecreeNo)
                {
                    latestCheckpoint = resp;
                }
            }

            if (latestCheckpoint != null && latestCheckpoint.CheckpointDecreeNo > this._proposerNote.GetMaximumCommittedDecreeNo() &&
                !string.IsNullOrEmpty(latestCheckpoint.CheckpointFile))
            {
                // get checkpoint from rmote node

                // get next checkpoint file
                var checkpointFilePath = _proposerNote.ProposeRoleMetaRecord?.CheckpointFilePath;
                if (checkpointFilePath == null)
                {
                    checkpointFilePath = ".\\storage\\" + _nodeInfo.Name + "_checkpoint.0000000000000001";
                }
                else
                {
                    int checkpointFileIndex = 0;
                    var baseName = ".\\storage\\" + _nodeInfo.Name + "_checkpoint";
                    var separatorIndex = checkpointFilePath.IndexOf(baseName);
                    if (separatorIndex != -1)
                    {
                        Int32.TryParse(checkpointFilePath.Substring(baseName.Length + 1), out checkpointFileIndex);
                        checkpointFileIndex++;
                    }
                    checkpointFilePath = baseName + "." + checkpointFileIndex.ToString("D16");
                }
                using (var fileStream = new FileStream(checkpointFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    UInt32 chunkSize = 4 * 1024 * 1024;
                    for (UInt64 off = 0; off < latestCheckpoint.CheckpointFileLength; off += chunkSize)
                    {
                        if (chunkSize > latestCheckpoint.CheckpointFileLength - off)
                        {
                            chunkSize = (UInt32)(latestCheckpoint.CheckpointFileLength - off);
                        }
                        var checkpointDataRequest = new ReadCheckpointDataRequest(
                            latestCheckpoint.CheckpointFile, off, chunkSize);
                        checkpointDataRequest.TargetNode = latestCheckpoint.SourceNode;
                        var checkpointDataResp = await Request(checkpointDataRequest) as ReadCheckpointDataResp;

                        // write the data to new checkpoint stream
                        fileStream.Write(checkpointDataResp.Data, 0, (int)checkpointDataResp.DataLength);
                    }

                }

                // update the checkpoint record
                // save new metadata
                await _proposerNote.Checkpoint(checkpointFilePath, latestCheckpoint.CheckpointDecreeNo);

                return true;
            }

            return false;
        }

        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
        }

        public Task TriggerCheckpoint()
        {
            // TODO: add parallel checkpoints support
            if (_catchupLogSize <= Persistence.LogSizeThreshold.CommitLogFileCheckpointThreshold)
            {
                return Task.CompletedTask;
            }
            _catchupLogSize = 0;

            var fakeMsg = new PaxosMessage();
            lock (_ongoingRequests)
            {
                if (Stop)
                {
                    return Task.CompletedTask;
                }
                _ongoingRequests.Add(fakeMsg);
            }

            var task = Task.Run(async () =>
            {
                try
                {
                    await Checkpoint();
                }
                finally
                {
                    lock (_ongoingRequests)
                    {
                        _ongoingRequests.Remove(fakeMsg);
                    }
                }
            });

            // do not care about the task, just let it run, when it's completed
            // it will remove the fake msg from onoging requests

            return task;
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            var fakeMsg = new PaxosMessage();
            lock (_ongoingRequests)
            {
                if (Stop)
                {
                    return null;
                }
                _ongoingRequests.Add(fakeMsg);
            }

            try
            {
                DecreeReadResult result = null;
                var maximumCommittedDecreeNo = _proposerNote.GetMaximumCommittedDecreeNo();
                if (decreeNo > maximumCommittedDecreeNo)
                {
                    result = new DecreeReadResult()
                    {
                        IsFound = false,
                        MaxDecreeNo = maximumCommittedDecreeNo,
                        CheckpointedDecreeNo = 0,
                        Decree = null
                    };

                    return result;
                }

                // committed decree can never be changed, no need to lock the decree no
                var committedDecreeInfo = await _proposerNote.GetCommittedDecree(decreeNo);
                result = new DecreeReadResult()
                {
                    IsFound = committedDecreeInfo.CommittedDecree != null,
                    MaxDecreeNo = maximumCommittedDecreeNo,
                    CheckpointedDecreeNo = committedDecreeInfo.CheckpointedDecreeNo,
                    Decree = committedDecreeInfo.CommittedDecree
                };

                return result;
            }
            finally
            {
                lock (_ongoingRequests)
                {
                    _ongoingRequests.Remove(fakeMsg);
                }
            }

        }

        public async Task<ProposeResult> Propose(PaxosDecree decree, ulong decreeNo)
        {
            var fakeMsg = new PaxosMessage();
            lock (_ongoingRequests)
            {
                if (Stop)
                {
                    return null;
                }
                _ongoingRequests.Add(fakeMsg);
            }

            try
            {
                ulong nextDecreeNo = decreeNo;

                TimeSpan collectLastVoteCostTimeInMs = new TimeSpan();
                TimeSpan voteCostTimeInMs = new TimeSpan();
                TimeSpan commitCostTimeInMs = new TimeSpan();


                do
                {
                    DateTime start = DateTime.Now;
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
                    Logger.Log("Propose a new propose, decreNo{0}", nextDecreeNo);

                    var lastVoteResult = await CollectLastVote(decree, nextDecreeNo);
                    Logger.Log("Got last vote result: isCommitted{0}, checkpointDecreeNo{1}", lastVoteResult.IsCommitted, lastVoteResult.CheckpointedDecreeNo);
                    /*
                    _proposeManager.RemovePropose(nextDecreeNo);
                    return new ProposeResult()
                    {
                        DecreeNo = nextDecreeNo,
                    };*/


                    DateTime collectLastVoteTime = DateTime.Now;
                    collectLastVoteCostTimeInMs += collectLastVoteTime - start;
                    var propose = _proposeManager.GetOngoingPropose(nextDecreeNo);
                    if (lastVoteResult.IsCommitted)
                    {
                        //_proposeManager.RemovePropose(nextDecreeNo);

                        // commit the propose
                        await CommitPropose(nextDecreeNo, lastVoteResult.OngoingPropose.LastTriedBallot);

                        // already committed
                        if (decreeNo == 0)
                        {
                            continue;
                        }

                        return new ProposeResult()
                        {
                            Decree = lastVoteResult.CommittedDecree,
                            DecreeNo = lastVoteResult.DecreeNo,
                            CollectLastVoteTimeInMs = collectLastVoteCostTimeInMs,
                            CommitTimeInMs = commitCostTimeInMs,
                            VoteTimeInMs = voteCostTimeInMs,
                            GetProposeCostTime = propose.GetProposeCostTime,
                            GetProposeLockCostTime = propose.GetProposeLockCostTime,
                            PrepareNewBallotCostTime = propose.PrepareNewBallotCostTime,
                            BroadcastQueryLastVoteCostTime = propose.BroadcastQueryLastVoteCostTime
                        };
                    }

                    var nextBallotNo = propose.GetNextBallot();
                    // check if stale message received
                    var nextAction = await propose.GetNextAction();
                    Logger.Log("Next propose action {0}", nextAction);
                    if (nextAction == Protocol.Propose.NextAction.CollectLastVote)
                    {
                        continue;
                    }
                    else if (nextAction == Protocol.Propose.NextAction.Commit)
                    {
                        await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);
                        commitCostTimeInMs += DateTime.Now - collectLastVoteTime;
                        if (decreeNo == 0)
                        {
                            Logger.Log("Commit decree{0} return from last vote, propose again", lastVoteResult.DecreeNo);
                            continue;
                        }

                        Logger.Log("Commit decree{0} committed", lastVoteResult.DecreeNo);

                        return new ProposeResult()
                        {
                            Decree = propose.GetCommittedDecree(),
                            DecreeNo = lastVoteResult.DecreeNo,
                            CollectLastVoteTimeInMs = collectLastVoteCostTimeInMs,
                            CommitTimeInMs = commitCostTimeInMs,
                            VoteTimeInMs = voteCostTimeInMs,
                            GetProposeCostTime = propose.GetProposeCostTime,
                            GetProposeLockCostTime = propose.GetProposeLockCostTime,
                            PrepareNewBallotCostTime = propose.PrepareNewBallotCostTime,
                            BroadcastQueryLastVoteCostTime = propose.BroadcastQueryLastVoteCostTime
                        };
                    }

                    // begin new ballot
                    Logger.Log("Prepre a new ballot");
                    var newBallotResult = await BeginNewBallot(lastVoteResult.DecreeNo, nextBallotNo);
                    var voteTime = DateTime.Now;
                    voteCostTimeInMs += voteTime - collectLastVoteTime;
                    propose = newBallotResult.OngoingPropose;
                    nextBallotNo = propose.GetNextBallot();
                    nextAction = await propose.GetNextAction();
                    Logger.Log("Prepre a new ballot got result, next action:{0}", nextAction);
                    if (nextAction == Protocol.Propose.NextAction.CollectLastVote)
                    {
                        Logger.Log("Prepre a new ballot result indicate need to recollect lastvote");
                        continue;
                    }
                    else if (nextAction == Protocol.Propose.NextAction.Commit)
                    {
                        var beforeCommit = DateTime.Now;
                        await CommitPropose(lastVoteResult.DecreeNo, nextBallotNo);
                        commitCostTimeInMs += DateTime.Now - beforeCommit;
                        Logger.Log("Decree committed");

                        return new ProposeResult()
                        {
                            Decree = propose.GetCommittedDecree(),
                            DecreeNo = lastVoteResult.DecreeNo,
                            CollectLastVoteTimeInMs = collectLastVoteCostTimeInMs,
                            CommitTimeInMs = commitCostTimeInMs,
                            VoteTimeInMs = voteCostTimeInMs,
                            GetProposeCostTime = propose.GetProposeCostTime,
                            GetProposeLockCostTime = propose.GetProposeLockCostTime,
                            PrepareNewBallotCostTime = propose.PrepareNewBallotCostTime,
                            BroadcastQueryLastVoteCostTime = propose.BroadcastQueryLastVoteCostTime
                        };
                    }
                } while (true);
            }
            finally
            {
                lock(_ongoingRequests)
                {
                    _ongoingRequests.Remove(fakeMsg);
                }
            }
        }


        protected override async Task<bool> DoDeliverPaxosMessage(PaxosMessage msg)
        {
            switch(msg.MessageType)
            {
                case PaxosMessageType.STALEBALLOT:
                    return await DeliverStaleBallotMessage(msg as StaleBallotMessage);
                case PaxosMessageType.LASTVOTE:
                    return await DeliverLastVoteMessage(msg as LastVoteMessage);
                case PaxosMessageType.VOTE:
                    return await DeliverVoteMessage(msg as VoteMessage);
                case PaxosMessageType.SUCCESS:
                    await DeliverSuccessMessage(msg as SuccessMessage);
                    return true;
                case PaxosMessageType.CheckpointSummaryReq:
                    return false;
                case PaxosMessageType.CheckpointSummaryResp:
                    return false;
                case PaxosMessageType.CheckpointDataReq:
                    return false;
                case PaxosMessageType.CheckpointDataResp:
                    return false;
            }

            return false;
        }

        protected override async Task<PaxosMessage> DoRequest(PaxosMessage request)
        {
            switch (request.MessageType)
            {
                case PaxosMessageType.CheckpointSummaryReq:
                    return await RequestCheckpointSummary();
                case PaxosMessageType.CheckpointDataReq:
                    return await RequestCheckpointData(request as ReadCheckpointDataRequest);
            }
            return null;
        }

        private async Task<bool> DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            var result = await ProcessStaleBallotMessageInternal(msg);
            if (result.NeedToCollectLastVote == false) //  no need to query last vote again
            {
                return false;
            }
            using (var autolock = await result.OngoingPropose.AcquireLock())
            {
                if (result.OngoingPropose.State == ProposeState.QueryLastVote ||
                    result.OngoingPropose.State == ProposeState.BeginNewBallot)
                {
                    var lastVoteResult = new ProposePhaseResult()
                    { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };
                    lock (result.OngoingPropose)
                    {
                        if (!result.OngoingPropose.Result.Task.IsCompleted)
                            result.OngoingPropose?.Result?.SetResult(lastVoteResult);
                    }
                }

            }
            //await BroadcastQueryLastVote(msg.DecreeNo, result.NextBallotNo);
            return true;
        }

        private async Task<bool> DeliverLastVoteMessage(LastVoteMessage msg)
        {
            var result = await ProcessLastVoteMessageInternal(msg);
            switch (result.Action)
            {
                case LastVoteMessageResult.ResultAction.None:
                    break;
                case LastVoteMessageResult.ResultAction.DecreeCommitted:
                case LastVoteMessageResult.ResultAction.NewBallotReadyToBegin:
                    {
                        var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                        if (propose == null)
                        {
                            return false;
                        }
                        using (var autolock = await propose.AcquireLock())
                        {
                            if (propose.State == ProposeState.QueryLastVote)
                            {
                                var phaseResult = new ProposePhaseResult()
                                {
                                    DecreeNo = msg.DecreeNo,
                                    OngoingPropose = propose,
                                    IsCommitted = LastVoteMessageResult.ResultAction.DecreeCommitted == result.Action,
                                    CommittedDecree = (LastVoteMessageResult.ResultAction.DecreeCommitted == result.Action) ? propose.Decree : null
                                };

                                lock (result.CommittedPropose)
                                {
                                    if (result.CommittedPropose.Result != null && !result.CommittedPropose.Result.Task.IsCompleted)
                                        result.CommittedPropose.Result.SetResult(phaseResult);
                                }
                            }
                            else if (propose.State == ProposeState.Commited)
                            {
                                var phaseResult = new ProposePhaseResult()
                                {
                                    DecreeNo = msg.DecreeNo,
                                    OngoingPropose = propose,
                                    CommittedDecree = propose.GetCommittedDecree(),
                                    IsCommitted = LastVoteMessageResult.ResultAction.DecreeCommitted == result.Action
                                };

                                lock (result.CommittedPropose)
                                {
                                    if (result.CommittedPropose.Result != null && !result.CommittedPropose.Result.Task.IsCompleted)
                                        result.CommittedPropose.Result.SetResult(phaseResult);
                                }
                            }
                            else
                            {
                                //Console.WriteLine("stale message");

                            }

                        }

                    }
                    break;
            }

            return true;
        }

        private async Task<bool> DeliverVoteMessage(VoteMessage msg)
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
                        using (var autolock = await propose.AcquireLock())
                        {
                            if (propose.State == ProposeState.BeginNewBallot)
                            {
                                var newBallotResult = new ProposePhaseResult()
                                { DecreeNo = msg.DecreeNo, OngoingPropose = _proposeManager.GetOngoingPropose(msg.DecreeNo) };

                                lock (propose)
                                {
                                    if (propose.Result != null && !propose.Result.Task.IsCompleted)
                                        propose.Result.SetResult(newBallotResult);

                                }
                            }

                        }
                    }
                    break;
            }

            return true;
        }

        private async Task DeliverSuccessMessage(SuccessMessage msg)
        {
            // commit in logs
            var position = await _proposerNote.CommitDecree(msg.DecreeNo, new PaxosDecree(msg.Decree));

            if (_notificationSubscriber != null)
                await _notificationSubscriber.UpdateSuccessfullDecree(msg.DecreeNo, new PaxosDecree(msg.Decree));

            _catchupLogSize += msg.Decree.Length;

            // check if need to checkpoint
            // TODO, remvoe it from critical path
            await TriggerCheckpoint();
        }

        private Task<CheckpointSummaryResp> RequestCheckpointSummary()
        {
            var filePath = _proposerNote.ProposeRoleMetaRecord?.CheckpointFilePath;
            if (string.IsNullOrEmpty(filePath))
            {
                return Task.FromResult(new CheckpointSummaryResp());
            }
            using (var checkpointFileStream = new FileStream(
                filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var resp = new CheckpointSummaryResp(filePath,
                    _proposerNote.ProposeRoleMetaRecord.DecreeNo, (UInt64)checkpointFileStream.Length);
                return Task.FromResult(resp);
            }
        }

        private async Task<ReadCheckpointDataResp> RequestCheckpointData(ReadCheckpointDataRequest req)
        {
            using (var checkpointFileStream = new FileStream(
                req.CheckpointFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var off = checkpointFileStream.Seek((long)req.CheckpointFileOff, SeekOrigin.Begin);
                if (off != (long)req.CheckpointFileOff)
                {
                    return new ReadCheckpointDataResp(req.CheckpointFile, (UInt64)off, 0, null);
                }

                var dataBuf = new byte[req.DataLength];
                var dataLen = await checkpointFileStream.ReadAsync(dataBuf, 0, (int)req.DataLength);
                return new ReadCheckpointDataResp(req.CheckpointFile, (UInt64)off, (UInt32)dataLen, dataBuf);
            }
        }

        /// <summary>
        /// public for test
        /// </summary>
        /// <param name="decree"></param>
        /// <param name="nextDecreeNo"></param>
        /// <returns></returns>
        public async Task<ProposePhaseResult> CollectLastVote(
            PaxosDecree decree,
            ulong nextDecreeNo)
        {
            DateTime begin = DateTime.Now;
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

            DateTime gotProposeTime = DateTime.Now;
            var getProposeCostTime = gotProposeTime - begin;
            propose.GetProposeCostTime += getProposeCostTime;

            ulong ballotNo = 0;
            var completionSource = new TaskCompletionSource<ProposePhaseResult>();

            using (var autoLock = await propose.AcquireLock())
            {
                DateTime gotProposeLockTime = DateTime.Now;
                var gotProposeLockCostTime = gotProposeLockTime - gotProposeTime;
                propose.GetProposeLockCostTime += gotProposeLockCostTime;

                var proposeResult = await GetProposeResult(nextDecreeNo);
                if (proposeResult != null)
                {
                    // already committed, return directly
                    return new ProposePhaseResult()
                    {
                        DecreeNo = nextDecreeNo,
                        CheckpointedDecreeNo = proposeResult.CheckpointedDecreeNo,
                        OngoingPropose = propose,
                        IsCommitted = true,
                        CommittedDecree = proposeResult.Decree
                    };
                }

                ballotNo = propose.PrepareNewBallot(decree);

                DateTime gotNewBallotTime = DateTime.Now;
                var gotNewBallotCostTime = gotNewBallotTime - gotProposeLockTime;
                propose.PrepareNewBallotCostTime += gotNewBallotCostTime;

                propose.Result = completionSource;

                if (ballotNo == 0)
                {
                    // cant be, send alert
                }
            }

            DateTime beforeBroadCastTime = DateTime.Now;
            await BroadcastQueryLastVote(nextDecreeNo, ballotNo);
            var broadCastCostTime = DateTime.Now - beforeBroadCastTime;
            propose.BroadcastQueryLastVoteCostTime += broadCastCostTime;

            return await completionSource.Task;
        }

        /// <summary>
        /// public for test
        /// </summary>
        /// <param name="decreeNo"></param>
        /// <param name="ballotNo"></param>
        /// <returns></returns>
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

        public bool NotifyLearners { get; set; }

        private async Task<Propose> CommitPropose(ulong decreeNo, ulong ballotNo)
        {
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            Persistence.AppendPosition position;
            using (var autoLock = await propose.AcquireLock())
            {
                if (!propose.Commit(ballotNo))
                {
                    // others may commit a new one with a different ballotNo
                    return null;
                }

                position = await _proposerNote.CommitDecree(decreeNo, propose.GetCommittedDecree());
            }

            await NotifyLearnersResult(decreeNo, ballotNo, propose.GetCommittedDecree());
            _proposeManager.RemovePropose(decreeNo);

            if (_notificationSubscriber != null)
                await _notificationSubscriber?.UpdateSuccessfullDecree(decreeNo, propose.GetCommittedDecree());

            if (propose.GetCommittedDecree().Data != null)
            {
                _catchupLogSize += propose.GetCommittedDecree().Data.Length;
            }
            // check if need to checkpoint
            // TODO, remvoe it from critical path
            await TriggerCheckpoint();

            // Max played 
            return propose;
        }

        private async Task<Propose> CommitProposeInLock(ulong decreeNo, ulong ballotNo)
        {
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            Persistence.AppendPosition position;
            if (!propose.Commit(ballotNo))
            {
                // others may commit a new one with a different ballotNo
                return null;
            }

            position = await _proposerNote.CommitDecree(decreeNo, propose.GetCommittedDecree());

            await NotifyLearnersResult(decreeNo, ballotNo, propose.GetCommittedDecree());
            _proposeManager.RemovePropose(decreeNo);

            if (_notificationSubscriber != null)
                await _notificationSubscriber?.UpdateSuccessfullDecree(decreeNo, propose.GetCommittedDecree());

            if (propose.GetCommittedDecree().Data != null)
            {
                _catchupLogSize += propose.GetCommittedDecree().Data.Length;
            }
            // check if need to checkpoint
            // TODO, remvoe it from critical path
            await TriggerCheckpoint();

            // Max played 
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
                    /*
                    if (propose.Commit(msg.BallotNo))
                    {
                        var committedDecree = propose.GetCommittedDecree();
                        await _proposerNote.CommitDecree(msg.DecreeNo, committedDecree);
                    }
                    else
                    {
                        // somebody else already committed it
                    }*/
                    //await CommitProposeInLock(msg.DecreeNo, msg.BallotNo);


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
            var committedDecreeInfo = await _proposerNote.GetCommittedDecree(decreeNo);
            if (committedDecreeInfo != null)
            {
                // already committed, return directly
                if (committedDecreeInfo.CommittedDecree != null ||
                    committedDecreeInfo.CheckpointedDecreeNo >= decreeNo)
                {
                    return new ProposeResult()
                    {
                        DecreeNo = decreeNo,
                        Decree = committedDecreeInfo.CommittedDecree,
                        CheckpointedDecreeNo = committedDecreeInfo.CheckpointedDecreeNo
                    };
                }
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
                var task = Task.Run(async () =>
                {
                    var nextBallotMessage = new NextBallotMessage();
                    nextBallotMessage.TargetNode = node.Name;
                    nextBallotMessage.DecreeNo = decreeNo;
                    nextBallotMessage.BallotNo = nextBallotNo;
                    return  SendPaxosMessage(nextBallotMessage);
                });

                tasks.Add(task);
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
                var task = Task.Run(async () =>
                {
                    var beginBallotMessage = new BeginBallotMessage();
                    beginBallotMessage.DecreeNo = decreeNo;
                    beginBallotMessage.BallotNo = ballotNo;
                    beginBallotMessage.TargetNode = node.Name;
                    beginBallotMessage.Decree = newBallotDecree.Data;
                    return SendPaxosMessage(beginBallotMessage);
                });

                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }

        private async Task NotifyLearnersResult(ulong decreeNo, ulong ballotNo, PaxosDecree decree)
        {
            if (!NotifyLearners)
            {
                return;
            }
            var tasks = new List<Task>();

            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var task = Task.Run(async () =>
                {
                    var successMessage = new SuccessMessage();
                    successMessage.TargetNode = node.Name;
                    successMessage.DecreeNo = decreeNo;
                    successMessage.BallotNo = ballotNo;
                    successMessage.Decree = decree.Data;

                    return SendPaxosMessage(successMessage);
                });

                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }

        private async Task Checkpoint()
        {
            // all the decree before decreeno has been checkpointed.
            // reserver the logs from the lowest position of the positions of decrees which are bigger than decreeNo
            if (_notificationSubscriber != null)
            {
                // get next checkpoint file
                var checkpointFilePath = _proposerNote.ProposeRoleMetaRecord?.CheckpointFilePath;
                if (checkpointFilePath == null)
                {
                    checkpointFilePath = ".\\storage\\" + _nodeInfo.Name + "_checkpoint.0000000000000001";
                }
                else
                {
                    int checkpointFileIndex = 0;
                    var baseName = ".\\storage\\" + _nodeInfo.Name + "_checkpoint";
                    var separatorIndex = checkpointFilePath.IndexOf(baseName);
                    if (separatorIndex != -1)
                    {
                        Int32.TryParse(checkpointFilePath.Substring(baseName.Length + 1), out checkpointFileIndex);
                        checkpointFileIndex++;
                    }
                    checkpointFilePath = baseName + "." + checkpointFileIndex.ToString("D16");
                }
                using (var fileStream = new FileStream(checkpointFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    var decreeNo = await _notificationSubscriber.Checkpoint(fileStream);

                    // save new metadata
                    await _proposerNote.Checkpoint(checkpointFilePath, decreeNo);

                }
            }
        }
    }

}
