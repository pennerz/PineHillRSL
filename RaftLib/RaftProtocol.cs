using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Raft.Message;
using PineHillRSL.Raft.Rpc;
using PineHillRSL.Raft.Notebook;
using PineHillRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.IO;

namespace PineHillRSL.Raft.Protocol
{
    public interface IPaxosStateMachine
    {
    }


    public class LeaderInfo
    {
        public LeaderInfo()
        { }

        public UInt64 Term { get; set; }
        public NodeAddress Leader { get; set; }
        public bool InVoting { get; set; }
    }
    public class RaftMessageQeuue : IAsyncDisposable
    {
        public class RequestItem
        {
            public RaftMessage Request { get; set; }
            //public Task<RaftMessage> Response { get; set; }

            public TaskCompletionSource<RaftMessage> Response { get; set; }
        }

        private SemaphoreSlim _lock = new SemaphoreSlim(3);
        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;
        private readonly NodeAddress _clientAddr;
        private List<RequestItem> _messageQueue = new List<RequestItem>();
        //private List<Task> _sendMsgTasks = new List<Task>();
        private bool _exit = false;

        public RaftMessageQeuue(NodeAddress serverAddr,NodeAddress clientAddr, RpcClient rpcClient)
        {
            _serverAddr = serverAddr;
            _clientAddr = clientAddr;
            _rpcClient = rpcClient;
        }

        public async ValueTask DisposeAsync()
        {
            _exit = true;
            //_lock.Release(1);
            //await _sendMsgTask;
        }

        public void AddRaftMessage(RequestItem request)
        {
            var raftMessage = request.Request;
            raftMessage.SourceNode = NodeAddress.Serialize(_serverAddr);
            lock (_lock)
            {
                _messageQueue.Add(request);
            }
            if (_lock.CurrentCount > 0)
            {
                var task = Task.Run(async () =>
                {
                    await MessageHandler();
                });

            }
        }

        public RequestItem PopMessage()
        {
            lock (_lock)
            {
                if (_messageQueue.Count == 0)
                {
                    return null;
                }
                var reqItem = _messageQueue[0];
                _messageQueue.RemoveAt(0);
                return reqItem;
            }
            /*
            var msgQueue = new List<RequestItem>();
            List<RequestItem> consumedMessages = null;
            lock (_lock)
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
                var aggregatedMsg = new AggregatedRaftMessage();
                aggregatedMsg.TargetNode = consumedMessages[0].TargetNode;
                foreach (var msg in consumedMessages)
                {
                    aggregatedMsg.AddRaftMessage(msg);
                }
                return aggregatedMsg;
            }
            */
        }

        public int MessageCount => _messageQueue.Count;

        private async Task MessageHandler()
        {
            try
            {
                await _lock.WaitAsync();
                bool retry = false;
                do
                {
                    RequestItem reqItem = null;
                    if (!retry)
                    {
                        reqItem = PopMessage();
                    }
                    if (reqItem == null)
                    {
                        return;
                    }
                    var raftMsg = reqItem.Request;
                    raftMsg.SourceNode = NodeAddress.Serialize(_clientAddr);
                    raftMsg.TargetNode = NodeAddress.Serialize(_serverAddr);
                    var raftRpcMsg = RaftMessageFactory.CreateRaftRpcMessage(raftMsg);
                    var rpcMsg = RaftRpcMessageFactory.CreateRpcRequest(raftRpcMsg);
                    rpcMsg.NeedResp = true;
                    var remoteAddr = NodeAddress.DeSerialize(raftMsg.TargetNode);

                    try
                    {
                        var respRpcMsg = await _rpcClient.SendRequest(remoteAddr, rpcMsg);
                        var respRaftRpcMsg = RaftRpcMessageFactory.CreateRaftRpcMessage(respRpcMsg);
                        var resp = RaftMessageFactory.CreateRaftMessage(respRaftRpcMsg);
                        reqItem.Response.SetResult(resp);
                    }
                    catch (Exception)
                    {
                        // time out, retry
                        retry = true;
                        continue;
                    }
                    retry = false;

                } while (true);
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    public class RaftFollower
    {
        private NodeAddress _leader;
        private DateTime _lastLeaderMsgTime = DateTime.MinValue;
        private UInt64 _term = 0;
        private UInt64 _logIndex = 0;
        private UInt64 _committedLogIndex = 0;
        private NodeAddress _candidate;
        private UInt64 _candateTerm = 0;
        private EntityNote _enityNotebook;

        public RaftFollower(EntityNote noteBook)
        {
            _enityNotebook = noteBook;
        }

        public NodeAddress GetLeader()
        {
            if (_leader == null)
            {
                return null;
            }

            if ((DateTime.Now - _lastLeaderMsgTime).TotalSeconds > 60)
            {
                return null;
            }

            return _leader;
        }

        public async Task<RaftMessage> DoRequest(RaftMessage request)
        {
            switch (request.MessageType)
            {
                case RaftMessageType.AppendEntityReq:
                    return await AppendEntity(request as AppendEntityReqMessage);
                case RaftMessageType.VoteReq:
                    return await VoteLeader(request as VoteReqMessage);
                default:
                    return null;
            }
        }

        public UInt64 Term => _term;
        public UInt64 LogIndex => _logIndex;
        public UInt64 CommitedLogIndex => _committedLogIndex;
        public UInt64 CandidateTerm => _candateTerm;
        public NodeAddress CandidateNode => _candidate;

        public void TestSetCurrentTerm(UInt64 curTerm)
        {
            _term = curTerm;
        }
        public void TestSetLogIndex(UInt64 logIndex)
        {
            _logIndex = logIndex;
        }
        public void TestSetCandidateTerm(UInt64 candidateTerm, NodeAddress candidate)
        {
            _candateTerm = candidateTerm;
            _candidate = candidate;
        }
        public void TestSetCommittedLogIndex(UInt64 committedLogIndex)
        {
            _committedLogIndex = committedLogIndex;
        }

        private async Task<AppendEntityRespMessage> AppendEntity(AppendEntityReqMessage req)
        {
            var resp = new AppendEntityRespMessage();
            if (req.Term < _term)
            {
                // new leader genertaed
                resp.Succeed = false;
                return resp;
            }

            if (req.CommittedLogIndex < _committedLogIndex)
            {
                resp.Succeed = false;
                return resp;
            }

            // write the entity
            await _enityNotebook.AppendEntity(req.Term, req.LogIndex, req.CommittedLogIndex,
                req.Data);

            if (req.Term > _term)
            {
                _term = req.Term;
                _leader = NodeAddress.DeSerialize(req.SourceNode);
            }
            if (req.CommittedLogIndex > _committedLogIndex)
            {
                _committedLogIndex = req.CommittedLogIndex;
            }
            _lastLeaderMsgTime = DateTime.Now;

            // write the latest log
            resp.Succeed = true;
            return resp;
        }

        private async Task<VoteRespMessage> VoteLeader(VoteReqMessage req)
        {
            var resp = new VoteRespMessage();
            if (req.Term <= _term)
            {
                resp.Succeed = false;
                return resp;
            }

            if (req.Term <= _candateTerm)
            {
                resp.Succeed = false;
                return resp;
            }

            if (req.LastLogEntryTerm < _term ||
                req.LastLogEntryIndex < _committedLogIndex)
            {
                resp.Succeed = false;
                return resp;
            }

            _candateTerm = req.Term;
            _candidate = NodeAddress.DeSerialize(req.SourceNode);

            resp.Succeed = true;
            return resp;
        }

    }

    public class RaftLeader
    {
        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;
        private readonly ConsensusCluster _cluster;

        private ConcurrentDictionary<string, RaftMessageQeuue> _raftMessageList;

        private Task _matainTask = null;
        private DateTime _lastLeaderClaim = DateTime.Now;

        private UInt64 _term = 0;
        private UInt64 _logIndex = 0;
        private UInt64 _committedLogIndex = 0;

        EntityNote _entityNoteBook;

        public RaftLeader(
            NodeAddress serverAddress,
            RpcClient rpcClient,
            ConsensusCluster cluster,
            UInt64 term, UInt64 logIndex,
            ConcurrentDictionary<string, RaftMessageQeuue> raftMesssageQueues,
            EntityNote entityNoteBook)
        {
            _rpcClient = rpcClient;
            _serverAddr = serverAddress;
            _cluster = cluster;
            _term = term;
            _logIndex = logIndex;
            _raftMessageList = raftMesssageQueues;
            _entityNoteBook = entityNoteBook;
            _matainTask = Task.Run(async () =>
            {
                await Mantain();
            });
        }

        public async Task<ProposeResult> Propose(ConsensusDecree decree, ulong decreeNo)
        {
            // if not leader
            // request append
            var appendRequest = new AppendEntityReqMessage()
            {
                Data = decree.Data,
                CommittedLogIndex = _committedLogIndex,
                LogIndex = _logIndex,
                Term = _term
            };
            do
            {
                try
                {
                    await _entityNoteBook.AppendEntity(_term, _logIndex, _committedLogIndex, decree.Data);
                    await AppendRequestToAllFollowers(appendRequest);
                    break;
                }
                catch (Exception e)
                {
                    throw e;
                }
            } while (true);

            return new ProposeResult()
            { };
        }

        public bool IsValid()
        {
            return true;
        }

        public UInt64 Term => _term;

        public UInt64 LogIndex => _logIndex;

        public bool EnableMatain { get; set; } = false;

        private async Task Mantain()
        {
            // 1. check if any leader
            do
            {
                if (!EnableMatain)
                {
                    await Task.Delay(30000); // delay 30s;
                    continue;
                }
                if ((DateTime.Now - _lastLeaderClaim).TotalSeconds < 60)
                {
                    // send empty append enity
                    var emptyRequest = new AppendEntityReqMessage();
                    lock (this)
                    {
                        emptyRequest.LogIndex = _logIndex;
                        emptyRequest.Term = _term;
                        ++_logIndex;
                    }

                    try
                    {
                        await AppendRequestToAllFollowers(emptyRequest);
                        _lastLeaderClaim = DateTime.Now;
                    }
                    catch (Exception e)
                    {
                        // exception, switch to follower
                        break;
                    }
                    await Task.Delay(30000); // delay 30s;
                    continue;
                }

            } while (true);

        }

        private async Task AppendRequestToAllFollowers(AppendEntityReqMessage append)
        {

            var taskList = new List<Task<RaftMessage>>();
            foreach (var follower in _cluster.Members)
            {
                var queue = GetMessageQueue(NodeAddress.Serialize(follower));
                var reqItem = new RaftMessageQeuue.RequestItem();
                reqItem.Request = append.DeepCopy();
                reqItem.Response = new TaskCompletionSource<RaftMessage>();
                taskList.Add(reqItem.Response.Task);
                queue.AddRaftMessage(reqItem);
            }

            int succeedCount = 0;
            do
            {
                var finishedTask = await Task.WhenAny(taskList.ToArray());
                taskList.Remove(finishedTask);
                var resp = finishedTask.Result as AppendEntityRespMessage;
                if (resp.Succeed)
                {
                    ++succeedCount;
                    if (succeedCount >= _cluster.Members.Count / 2 + 1)
                    {
                        // succedd, no need to wait for others
                        return;
                    }
                }
                else
                {
                    // new leader generated
                    throw new Exception("new leader");
                }
            } while (true);
            throw new Exception("fail");
        }

        private RaftMessageQeuue GetMessageQueue(string targetNode)
        {
            RaftMessageQeuue messageQueue = null;
            do
            {
                if (_raftMessageList.TryGetValue(targetNode, out messageQueue))
                {
                    return messageQueue;
                }
                var destAddr = NodeAddress.DeSerialize(targetNode);
                messageQueue = new RaftMessageQeuue(destAddr, _serverAddr, _rpcClient);
                _raftMessageList.TryAdd(targetNode, messageQueue);
            } while (true);
        }
    }

    public class RaftCandidate
    {
        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;
        private readonly ConsensusCluster _cluster;
        private UInt64 _curTerm = 0;
        private UInt64 _lastLogEntryTerm = 0;
        private UInt64 _lastLogEntryIndex = 0;

        private ConcurrentDictionary<string, RaftMessageQeuue> _raftMessageList;


        public RaftCandidate(
            NodeAddress serverAddress,
            RpcClient rpcClient,
            ConsensusCluster cluster,
            UInt64 curTerm,
            UInt64 lastLogEntryTerm,
            UInt64 lastLogEntryIndex,
            ConcurrentDictionary<string, RaftMessageQeuue> raftMesssageQueues)
        {
            _rpcClient = rpcClient;
            _serverAddr = serverAddress;
            _cluster = cluster;
            _curTerm = curTerm;
            _lastLogEntryIndex = lastLogEntryIndex;
            _lastLogEntryTerm = lastLogEntryTerm;
            _raftMessageList = raftMesssageQueues;
        }

        public async Task RequestLeaderVoteFromAllFollowers()
        {
            VoteReqMessage leadVoteReq = new VoteReqMessage();
            leadVoteReq.Term = _curTerm;

            leadVoteReq.LastLogEntryTerm = _lastLogEntryTerm;
            leadVoteReq.LastLogEntryIndex = _lastLogEntryIndex;
            leadVoteReq.LogIndex = _lastLogEntryIndex;

            var taskList = new List<Task<RaftMessage>>();
            foreach (var follower in _cluster.Members)
            {
                var queue = GetMessageQueue(NodeAddress.Serialize(follower));
                var reqItem = new RaftMessageQeuue.RequestItem();
                reqItem.Request = leadVoteReq.DeepCopy();
                reqItem.Response = new TaskCompletionSource<RaftMessage>();
                taskList.Add(reqItem.Response.Task);
                queue.AddRaftMessage(reqItem);
            }

            int succeedCount = 0;
            do
            {
                var finishedTask = await Task.WhenAny(taskList.ToArray());
                taskList.Remove(finishedTask);
                var resp = finishedTask.Result as VoteRespMessage;
                if (resp.Succeed)
                {
                    ++succeedCount;
                    if (succeedCount >= _cluster.Members.Count / 2 + 1)
                    {
                        // succedd, no need to wait for others
                        return;
                    }
                }
                if (taskList.Count <= _cluster.Members.Count / 2 - 1)
                {
                    break;
                }
            } while (true);
            throw new Exception("fail");
        }

        public UInt64 CurTerm => _curTerm;
        public UInt64 LastLogEntryTerm => _lastLogEntryTerm;
        public UInt64 LastLogEntryIndex => _lastLogEntryIndex;

        private RaftMessageQeuue GetMessageQueue(string targetNode)
        {
            RaftMessageQeuue messageQueue = null;
            do
            {
                if (_raftMessageList.TryGetValue(targetNode, out messageQueue))
                {
                    return messageQueue;
                }
                var targetAddr = NodeAddress.DeSerialize(targetNode);
                messageQueue = new RaftMessageQeuue(targetAddr, _serverAddr, _rpcClient);
                _raftMessageList.TryAdd(targetNode, messageQueue);
            } while (true);
        }
    }

    public class RaftRole : IAsyncDisposable
    {
        enum RaftRoleType { Leader, Follower, Candidate };


        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;

        private RaftLeader _leader = null;
        private RaftFollower _follower = null;
        private RaftCandidate _candidate = null;

        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private List<RaftMessage> _procssingMsgList = new List<RaftMessage>();
        private readonly ConsensusCluster _cluster;
        private RaftRoleType _role = RaftRoleType.Follower;
        private DateTime _lastLeaderSyncTime = DateTime.MinValue;

        IConsensusNotification _notificationSubscriber;

        private ConcurrentDictionary<string, RaftMessageQeuue> _raftMessageList;
        private Task _matainTask = null;

        private UInt64 _term = 0;
        private UInt64 _logIndex = 0;

        private EntityNote _entityNote = null;

        private UInt64 _catchupLogSize = 0;

        public RaftRole(
            NodeAddress serverAddress,
            RpcClient rpcClient,
            ConsensusCluster cluster,
            EntityNote entityNote)
        {
            if (serverAddress == null) throw new ArgumentNullException("nodeInfo");
            if (rpcClient == null) throw new ArgumentNullException("rpcClient");
            Stop = false;
            _serverAddr = serverAddress;
            _rpcClient = rpcClient;
            _cluster = cluster;
            _raftMessageList = new ConcurrentDictionary<string, RaftMessageQeuue>();

            _entityNote = entityNote;
            _follower = new RaftFollower(_entityNote);
            _leader = new RaftLeader(_serverAddr, _rpcClient, _cluster, _term, _logIndex, _raftMessageList, _entityNote);
            EnableMatain = false;
            _matainTask = Task.Run(async () =>
            {
                await Mantain();
            });
        }

        public async ValueTask DisposeAsync()
        {

        }

        public async Task Load(DataSource dataSource)
        {
            int catchupLogSize = 0;
            /*
            do
            {
                await _entityNote.Load();
                //_proposeManager.ResetBaseDecreeNo(await _proposerNote.GetMaximumCommittedDecreeNo());
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
                        catch (Exception)
                        {
                        }

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
            */
        }

        public bool Stop { get; set; }
        public bool WaitRpcResp { get; set; }

        public bool EnableMatain { get; set; }

        public NodeAddress ServerAddress => _serverAddr;

        public async Task<ProposeResult> Propose(ConsensusDecree decree, ulong decreeNo)
        {
            // if not leader
            do
            {
                if (_follower.GetLeader() != null && !_follower.GetLeader().Equals(_serverAddr))
                {
                    // leader is others
                    var result = new ProposeResult()
                    {
                        Leader = NodeAddress.Serialize(_follower.GetLeader()),
                        Retry = true
                    };
                    return result;

                }

                // this is leader
                if (_leader != null && _leader.IsValid())
                {
                    // request append
                    try
                    {
                        await _leader.Propose(decree, decreeNo);
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                    return new ProposeResult()
                    { };
                }

                // no valid leader
                await VoteLeader();

            } while (true);
        }

        public async Task<bool> HandleRaftMessage(RaftMessage msg)
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
                bool ret = await DoDeliverRaftMessage(msg);
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

        public async Task<RaftMessage> HandleRequest(RaftMessage request)
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

        private async Task Mantain()
        {
            // 1. check if any leader
            do
            {
                if (EnableMatain)
                {
                    var leader = _follower.GetLeader();
                    if (leader == null)
                    {
                        await VoteLeader();
                    }
                }
                await Task.Delay(5000);
            } while (true);

        }

        public async Task VoteLeader()
        {
            do
            {
                var leader = _follower.GetLeader();
                if (leader != null)
                {
                    return;
                }

                // no leader
                _candidate = new RaftCandidate(
                    _serverAddr, _rpcClient, _cluster,
                    _follower.Term + 1,
                    _follower.Term, _follower.LogIndex,
                    _raftMessageList);
                try
                {
                    await _candidate.RequestLeaderVoteFromAllFollowers();
                    _leader = new RaftLeader(_serverAddr, _rpcClient, _cluster,
                        _candidate.CurTerm, _candidate.LastLogEntryIndex,
                        _raftMessageList, _entityNote);
                    return;
                }
                catch (Exception e)
                {
                    // fail to promoted a leader

                    // if a leader found

                    // if no leader found

                }
                await Task.Delay(5000);

            } while (true);

        }

        protected Task<bool> DoDeliverRaftMessage(RaftMessage msg)
        {
            return Task.FromResult(false);
        }

        protected async Task<RaftMessage> DoRequest(RaftMessage request)
        {
            if (request == null)
            {
                return null;
            }

            // request need to be handled by follower
            return await _follower.DoRequest(request);
        }


        public async Task WaitForAllMessageSent()
        {
            foreach(var messageList in _raftMessageList)
            {
                while(messageList.Value.MessageCount > 0)
                {
                    await Task.Delay(100);
                }
            }
            await Task.Delay(100);
        }

    }

}
