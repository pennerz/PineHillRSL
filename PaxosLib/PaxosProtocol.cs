using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Message;
using PineHillRSL.Paxos.Notebook;
using PineHillRSL.Paxos.Request;
using PineHillRSL.Paxos.Rpc;
using PineHillRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.IO;

namespace PineHillRSL.Paxos.Protocol
{
    public interface IPaxosStateMachine
    {
    }

    public abstract class PaxosRole : IAsyncDisposable
    {
        private class PaxosMessageQeuue : IAsyncDisposable
        {
            private SemaphoreSlim _lock = new SemaphoreSlim(3);
            private readonly RpcClient _rpcClient;
            private readonly NodeAddress _serverAddr;
            private List<PaxosMessage> _messageQueue = new List<PaxosMessage>();
            private Task _sendMsgTask = null;
            private bool _exit = false;

            public PaxosMessageQeuue(NodeAddress serverAddr, RpcClient rpcClient)
            {
                _serverAddr = serverAddr;
                _rpcClient = rpcClient;

                /*
                _sendMsgTask = Task.Run(async () =>
                {
                    while(true)
                    {
                        try
                        {
                            await _lock.WaitAsync();
                            if (_exit)
                            {
                                break;
                            }
                            do
                            {
                                var paxosMsg = PopMessage();
                                if (paxosMsg == null)
                                {
                                    continue;
                                }

                                var task = Task.Run(() =>
                                {
                                    paxosMsg.SourceNode = NodeAddress.Serialize(_serverAddr);
                                    var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMsg);
                                    var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                                    rpcMsg.NeedResp = false;
                                    var remoteAddr = NodeAddress.DeSerialize(paxosMsg.TargetNode);

                                    var task = _rpcClient.SendRequest(remoteAddr, rpcMsg);

                                });

                            } while (true);
                        }
                        catch(Exception)
                        {
                        }

                    }
                });
                */
            }

            public  async ValueTask DisposeAsync()
            {
                _exit = true;
                //_lock.Release(1);
                //await _sendMsgTask;
            }

            public void AddPaxosMessage(PaxosMessage paxosMessage)
            {
                paxosMessage.SourceNode = NodeAddress.Serialize(_serverAddr);
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
                                paxosMsg.SourceNode = NodeAddress.Serialize(_serverAddr);
                                var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(paxosMsg);
                                var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                                rpcMsg.NeedResp = false;
                                var remoteAddr = NodeAddress.DeSerialize(paxosMsg.TargetNode);

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
                    if (_messageQueue.Count == 0)
                    {
                        return null;
                    }
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
        private readonly NodeAddress _serverAddr;
        private ConcurrentDictionary<string, PaxosMessageQeuue> _paxosMessageList =
            new ConcurrentDictionary<string, PaxosMessageQeuue>();
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        private List<PaxosMessage> _procssingMsgList = new List<PaxosMessage>();

        public PaxosRole(
            NodeAddress serverAddress,
            RpcClient rpcClient)
        {
            if (serverAddress == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            Stop = false;
            _serverAddr = serverAddress;
            _rpcClient = rpcClient;
        }

        public async ValueTask DisposeAsync()
        {
            foreach(var msgQueue in _paxosMessageList)
            {
                await msgQueue.Value.DisposeAsync();
            }
        }

        public bool Stop { get; set; }
        public bool WaitRpcResp { get; set; }

        public NodeAddress ServerAddress => _serverAddr;

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


        protected Task SendPaxosMessage(PaxosMessage paxosMessage)
        {
            if (paxosMessage == null)
            {
                return Task.CompletedTask;
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
            return Task.CompletedTask;
        }

        protected async Task<PaxosMessage> Request(PaxosMessage request)
        {
            request.SourceNode = NodeAddress.Serialize(_serverAddr);
            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(request);
            var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
            rpcMsg.NeedResp = true;
            var remoteAddr = NodeAddress.DeSerialize(request.TargetNode);

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
                messageQueue = new PaxosMessageQeuue(_serverAddr, _rpcClient);
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
    public class VoterRole : PaxosRole, IAsyncDisposable
    {
        private readonly NodeAddress _serverAddr;
        private readonly VoterNote _note;
        private readonly ProposerNote _ledger;
        private IConsensusNotification _notificationSubscriber;
        private Task _truncateLogTask = null;
        private bool _exit = false;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

        private List<PaxosMessage> _ongoingMessages = new List<PaxosMessage>();

        public VoterRole(
            NodeAddress serverAddress,
            ConsensusCluster cluster,
            RpcClient rpcClient,
            VoterNote voterNote,
            ProposerNote ledger)
            : base(serverAddress, rpcClient)
        {
            if (voterNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _serverAddr = serverAddress;
            _note = voterNote;
            _ledger = ledger;

            WaitRpcResp = false;

            _truncateLogTask = Task.Run(async () =>
            {
                int truncateRound = 0;
                while(!_exit)
                {
                    try
                    {
                        await Task.Delay(1000, _cancel.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                    if (truncateRound < 600)
                    {
                        truncateRound++;
                        continue;
                    }
                    truncateRound = 0;
                    var maxDecreeNo = await _ledger.GetMaximumCommittedDecreeNo();
                    var position = _note.GetMaxPositionForDecrees(maxDecreeNo);
                    _note.Truncate(maxDecreeNo, position);
                }
            });
        }

        public new async ValueTask DisposeAsync()
        {
            _exit = true;
            Stop = true;
            _cancel.Cancel();
            await base.DisposeAsync();

            await _truncateLogTask;
        }

        public void SubscribeNotification(IConsensusNotification listener)
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

        private Task DeliverSuccessMessage(SuccessMessage msg)
        {
            lock(_ongoingMessages)
            {
                if (_exit)
                {
                    return Task.CompletedTask;
                }
                _ongoingMessages.Add(msg);
            }

            // commit in logs
            //await _ledger.CommitDecree(msg.DecreeNo, new ConsensusDecree(msg.Decree));

            //if (_notificationSubscriber != null)
            //    await _notificationSubscriber.UpdateSuccessfullDecree(msg.DecreeNo, new ConsensusDecree(msg.Decree));

            // for test, control memory comsumtion
            _note.RemoveBallotInfo(msg.DecreeNo);

            lock(_ongoingMessages)
            {
                _ongoingMessages.Remove(msg);
            }

            return Task.CompletedTask;
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
                oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, new ConsensusDecree(msg.Decree));

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

    public class ProposerRole : PaxosRole, IAsyncDisposable
    {
        private readonly NodeAddress _serverAddr;
        private readonly ConsensusCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly ProposeManager _proposeManager;

        IConsensusNotification _notificationSubscriber;

        private List<PaxosMessage> _ongoingRequests = new List<PaxosMessage>();
        private ulong _catchupLogSize = 0;
        private Task _checkpointTask = null;

        //public enum DataSource { Local, Cluster};

        public ProposerRole(
            NodeAddress serverAddr,
            ConsensusCluster cluster,
            RpcClient rpcClient,
            ProposerNote proposerNote,
            ProposeManager proposerManager)
            : base(serverAddr, rpcClient)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (serverAddr == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");

            _serverAddr = serverAddr;
            _cluster = cluster;
            _proposerNote = proposerNote;
            _proposeManager = proposerManager;
            Stop = false;
            NotifyLearners = true;
        }

        public new async ValueTask DisposeAsync()
        {
            Stop = true;
            await base.DisposeAsync();
            if (_checkpointTask != null)
            {
                await _checkpointTask;
            }
            while (true)
            {
                lock(_ongoingRequests)
                {
                    if (_ongoingRequests.Count == 0)
                    {
                        break;
                    }
                }
                await Task.Delay(100);
            }
        }

        public async Task Load(DataSource dataSource = DataSource.Local)
        {
            int catchupLogSize = 0;
            do
            {
                await _proposerNote.Load();
                _proposeManager.ResetBaseDecreeNo(await _proposerNote.GetMaximumCommittedDecreeNo());
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
                        ConsensusDecree fakeDecree = new ConsensusDecree();
                        try
                        {
                            await Propose(fakeDecree, 0);
                        }
                        catch(Exception)
                        {
                        }

                    }

                    // catchup 
                    foreach (var committedDecree in _proposerNote.CommittedDecrees)
                    {
                        //await _notificationSubscriber.UpdateSuccessfullDecree(committedDecree.Key, committedDecree.Value);
                        if (committedDecree.Value.Data != null)
                        {
                            catchupLogSize += committedDecree.Value.Data.Length;
                        }
                    }
                }

                if (dataSource == DataSource.Local)
                {
                    break;
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
            _catchupLogSize = (ulong)catchupLogSize;
        }

        private async Task<bool> LoadCheckpointFromRemoteNodes()
        {
            var summaryRequestTaskList = new List<Task<PaxosMessage>>();
            foreach (var node in _cluster.Members)
            {
                if (node.Equals(_serverAddr))
                {
                    continue;
                }
                var checkpointSummaryRequest = new CheckpointSummaryRequest();
                checkpointSummaryRequest.TargetNode = NodeAddress.Serialize(node);
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

            if (latestCheckpoint != null && latestCheckpoint.CheckpointDecreeNo > await _proposerNote.GetMaximumCommittedDecreeNo() &&
                !string.IsNullOrEmpty(latestCheckpoint.CheckpointFile))
            {
                // get checkpoint from rmote node

                // get next checkpoint file
                var checkpointFilePath = _proposerNote.ProposeRoleMetaRecord?.CheckpointFilePath;
                if (checkpointFilePath == null)
                {
                    checkpointFilePath = ".\\storage\\" + Node.PaxosNode.GetInstanceName(_serverAddr) + "_checkpoint.0000000000000001";
                }
                else
                {
                    int checkpointFileIndex = 0;
                    var baseName = ".\\storage\\" + Node.PaxosNode.GetInstanceName(_serverAddr) + "_checkpoint";
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

        public void SubscribeNotification(IConsensusNotification listener)
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
                var maximumCommittedDecreeNo = await _proposerNote.GetMaximumCommittedDecreeNo();
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

        //                                                                commited returned
        //                                       |--------------------------------------------------------------------------------
        //                                       |                                                                              \|/
        // init ->  collect_last_vote -> wait_for_last_vote -> beginnewballot -> wait_for_new_ballot_result ->ReadyCommit -->  committed  Checkpointed
        //            /|\  /|\                   |                                    | |                                       /|\
        //             |    ---------------------|                                    | |----------------------------------------|
        //             |              timeout                                         |     others has new propose and committed
        //             |                                                              |
        //             |______________________________________________________________|
        //              stale message indicate ballot already occupied by new proposer
        

        public async Task<ProposeResult> Propose(ConsensusDecree decree, ulong decreeNo)
        {
            var fakeMsg = new PaxosMessage();
            lock (_ongoingRequests)
            {
                if (Stop)
                {
                    return null;
                }
                _ongoingRequests.Add(fakeMsg);  // for count ongoing requests in paxos protocol
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
                    }
                    else
                    {
                        nextDecreeNo = decreeNo;
                    }
                    _proposeManager.AddPropose(nextDecreeNo, (ulong)_cluster.Members.Count);
                    Logger.Log("Propose a new propose, decreNo{0}", nextDecreeNo);

                    System.Diagnostics.Stopwatch timer = null;
                    ProposePhaseResult result = null;
                    var propose = _proposeManager.GetOngoingPropose(nextDecreeNo);
                    do
                    {
                        switch (propose.State)
                        {
                            case ProposeState.QueryLastVote:
                                timer = System.Diagnostics.Stopwatch.StartNew();
                                result = await CollectLastVote(decree, nextDecreeNo);
                                timer.Stop();
                                collectLastVoteCostTimeInMs = timer.Elapsed;
                                //if (result.IsCommitted && result.CheckpointedDecreeNo >= nextDecreeNo)
                                //{
                                //    throw new Exception("Decree checkpointed");
                                //}
                                break;
                            case ProposeState.WaitForLastVote:
                                break;
                            case ProposeState.BeginNewBallot:
                                timer = System.Diagnostics.Stopwatch.StartNew();
                                result = await BeginNewBallot(nextDecreeNo, propose.GetNextBallot());
                                timer.Stop();
                                voteCostTimeInMs = timer.Elapsed;
                                break;
                            case ProposeState.WaitForNewBallotVote:
                                break;
                            case ProposeState.ReadyToCommit:
                                timer = System.Diagnostics.Stopwatch.StartNew();
                                await CommitPropose(nextDecreeNo, propose.GetNextBallot());
                                timer.Stop();
                                commitCostTimeInMs = timer.Elapsed;
                                break;
                            case ProposeState.Commited:
                                // ready 
                                break;
                            case ProposeState.DecreeCheckpointed:
                                throw new Exception("Decree checkpointed");
                            default:
                                break;
                        }
                        if (propose.State == ProposeState.Commited)
                        {
                            break;
                        }
                    } while (true);

                    if (propose.State != ProposeState.Commited)
                    {
                        // fail to commit
                        //return 
                    }

                    if (decree != propose.Decree)
                    {
                        if (decreeNo == 0)
                        {
                            continue;
                        }
                    }

                    return new ProposeResult()
                    {
                        Decree = propose.Decree,
                        DecreeNo = nextDecreeNo,
                        CollectLastVoteTimeInMs = collectLastVoteCostTimeInMs,
                        CommitTimeInMs = commitCostTimeInMs,
                        VoteTimeInMs = voteCostTimeInMs,
                        GetProposeCostTime = propose.GetProposeCostTime,
                        GetProposeLockCostTime = propose.GetProposeLockCostTime,
                        PrepareNewBallotCostTime = propose.PrepareNewBallotCostTime,
                        BroadcastQueryLastVoteCostTime = propose.BroadcastQueryLastVoteCostTime
                    };

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
            var messageReult = await ProcessStaleBallotMessageInternal(msg);
            if(messageReult == Protocol.Propose.MessageHandleResult.Ready)
            {
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }
                using (var autolock = await propose.AcquireLock())
                {
                    var result = new ProposePhaseResult()
                    { };
                    lock (propose)
                    {
                        if (propose.Result != null && propose.Result.Task != null &&
                            !propose.Result.Task.IsCompleted)
                            propose?.Result?.SetResult(result);
                    }
                }
            }
            return true;
        }

        private async Task<bool> DeliverLastVoteMessage(LastVoteMessage msg)
        {
            var messageReult = await ProcessLastVoteMessageInternal(msg);
            if (messageReult == Protocol.Propose.MessageHandleResult.Ready)
            {
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }
                using (var autolock = await propose.AcquireLock())
                {
                    // not care about the result anymore
                    var phaseResult = new ProposePhaseResult();

                    if (propose.Result != null && !propose.Result.Task.IsCompleted)
                        propose.Result.SetResult(phaseResult);
                }

            }
            return true;
        }

        private async Task<bool> DeliverVoteMessage(VoteMessage msg)
        {
            var messageReult = await ProcessVoteMessageInternal(msg);
            if (messageReult == Protocol.Propose.MessageHandleResult.Ready)
            {
                var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
                if (propose == null)
                {
                    return false;
                }
                using (var autolock = await propose.AcquireLock())
                {
                    if (propose.State == ProposeState.ReadyToCommit)
                    {
                        var newBallotResult = new ProposePhaseResult()
                        { };

                        lock (propose)
                        {
                            if (propose.Result != null && !propose.Result.Task.IsCompleted)
                                propose.Result.SetResult(newBallotResult);
                        }
                    }
                }
            }
            return true;
        }

        private async Task DeliverSuccessMessage(SuccessMessage msg)
        {
            // commit in logs
            var position = await _proposerNote.CommitDecree(msg.DecreeNo, new ConsensusDecree(msg.Decree));

            if (_notificationSubscriber != null)
                await _notificationSubscriber.UpdateSuccessfullDecree(msg.DecreeNo, new ConsensusDecree(msg.Decree));

            if (msg.Decree != null)
                _catchupLogSize += (ulong)msg.Decree.Length;

            // check if need to checkpoint
            // TODO, remvoe it from critical path
            lock(this)
            {
                if (_checkpointTask != null && !_checkpointTask.IsCompleted)
                {
                    return;
                }
                _checkpointTask = TriggerCheckpoint();
            }
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
            ConsensusDecree decree,
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


                // TODO: query the last vote from the node itself
                /*
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
                }*/

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
            ConsensusDecree newBallotDecree = null;
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            if (propose == null)
            {
                // send alert
                return new ProposePhaseResult()
                {
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

                position = await _proposerNote.CommitDecree(decreeNo, propose.Decree);
            }
            if (propose.Decree == null || propose.Decree.Data == null)
            {
                Console.WriteLine("error");
            }

            await NotifyLearnersResult(decreeNo, ballotNo, propose.Decree);
            _proposeManager.RemovePropose(decreeNo);

            if (_notificationSubscriber != null)
                await _notificationSubscriber?.UpdateSuccessfullDecree(decreeNo, propose.Decree);

            if (propose.Decree.Data != null)
            {
                _catchupLogSize += (ulong)propose.Decree.Data.Length;
            }
            // check if need to checkpoint
            // TODO, remvoe it from critical path
            lock(this)
            {
                if (_checkpointTask == null || _checkpointTask.IsCompleted)
                {
                    _checkpointTask = TriggerCheckpoint(); // no need to wait
                }
            }

            // Max played 
            return propose;
        }

        /*
        private async Task<Propose> CommitProposeInLock(ulong decreeNo, ulong ballotNo)
        {
            var propose = _proposeManager.GetOngoingPropose(decreeNo);
            Persistence.AppendPosition position;
            if (!propose.Commit(ballotNo))
            {
                // others may commit a new one with a different ballotNo
                return null;
            }

            position = await _proposerNote.CommitDecree(decreeNo, propose.Decree);

            await NotifyLearnersResult(decreeNo, ballotNo, propose.Decree);
            _proposeManager.RemovePropose(decreeNo);

            if (_notificationSubscriber != null)
                await _notificationSubscriber?.UpdateSuccessfullDecree(decreeNo, propose.Decree);

            if (propose.Decree.Data != null)
            {
                _catchupLogSize += (ulong)propose.Decree.Data.Length;
            }
            // check if need to checkpoint
            lock(this)
            {
                if (_checkpointTask == null || _checkpointTask.IsCompleted)
                {
                    _checkpointTask = TriggerCheckpoint();
                }
            }

            // Max played 
            return propose;
        }*/

        private async Task<Propose.MessageHandleResult> ProcessStaleBallotMessageInternal(StaleBallotMessage msg)
        {
            // in case committed, ongoing propose could be cleaned
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                return Protocol.Propose.MessageHandleResult.Continue;
            }

            // QueryLastVote || BeginNewBallot
            using (var autoLock = await propose.AcquireLock())
            {
                return propose.AddStaleBallotMessage(msg);
            }

        }

        private async Task<Propose.MessageHandleResult> ProcessLastVoteMessageInternal(LastVoteMessage msg)
        {
            // in case committed, ongoing propose could be cleaned
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                return Protocol.Propose.MessageHandleResult.Continue;
            }

            using (var autoLock = await propose.AcquireLock())
            {
                return propose.AddLastVoteMessage(msg);
            }
        }

        private async Task<Propose.MessageHandleResult> ProcessVoteMessageInternal(VoteMessage msg)
        {
            var propose = _proposeManager.GetOngoingPropose(msg.DecreeNo);
            if (propose == null)
            {
                // propose may already be committed
                return Protocol.Propose.MessageHandleResult.Continue;
            }
            using (var autoLock = await propose.AcquireLock())
            {
                return propose.AddVoteMessage(msg);
            }
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
                if (node.Equals(_serverAddr))
                {
                    //continue;
                }
                var task = Task.Run(async () =>
                {
                    var nextBallotMessage = new NextBallotMessage();
                    nextBallotMessage.TargetNode = NodeAddress.Serialize(node);
                    nextBallotMessage.DecreeNo = decreeNo;
                    nextBallotMessage.BallotNo = nextBallotNo;
                    await  SendPaxosMessage(nextBallotMessage);
                });

                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }

        private async Task BroadcastBeginNewBallot(UInt64 decreeNo, UInt64 ballotNo, ConsensusDecree newBallotDecree)
        {
            var tasks = new List<Task>();
            foreach (var node in _cluster.Members)
            {
                if (node.Equals(_serverAddr))
                {
                    //continue;
                }
                var task = Task.Run(async () =>
                {
                    var beginBallotMessage = new BeginBallotMessage();
                    beginBallotMessage.DecreeNo = decreeNo;
                    beginBallotMessage.BallotNo = ballotNo;
                    beginBallotMessage.TargetNode = NodeAddress.Serialize(node);
                    beginBallotMessage.Decree = newBallotDecree.Data;
                    await SendPaxosMessage(beginBallotMessage);
                });

                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }

        private async Task NotifyLearnersResult(ulong decreeNo, ulong ballotNo, ConsensusDecree decree)
        {
            if (!NotifyLearners)
            {
                return;
            }
            var tasks = new List<Task>();

            foreach (var node in _cluster.Members)
            {
                if (node.Equals(_serverAddr))
                {
                    continue;
                }
                var task = Task.Run(async () =>
                {
                    var successMessage = new SuccessMessage();
                    successMessage.TargetNode = NodeAddress.Serialize(node);
                    successMessage.DecreeNo = decreeNo;
                    successMessage.BallotNo = ballotNo;
                    successMessage.Decree = decree.Data;

                    await SendPaxosMessage(successMessage);
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
                    checkpointFilePath = ".\\storage\\" + Node.PaxosNode.GetInstanceName(_serverAddr) + "_checkpoint.0000000000000001";
                }
                else
                {
                    int checkpointFileIndex = 0;
                    var baseName = ".\\storage\\" + Node.PaxosNode.GetInstanceName(_serverAddr) + "_checkpoint";
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
