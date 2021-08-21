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

    public abstract class RaftMessageQeuue : IAsyncDisposable
    {
        public class RequestItem
        {
            public RaftMessage Request { get; set; }
            //public Task<RaftMessage> Response { get; set; }

            public TaskCompletionSource<RaftMessage> Response { get; set; }
        }

        protected SemaphoreSlim _lock = new SemaphoreSlim(1);
        private List<RequestItem> _messageQueue = new List<RequestItem>();
        //private List<Task> _sendMsgTasks = new List<Task>();
        private bool _exit = false;

        private readonly NodeAddress _serverAddr;

        public RaftMessageQeuue(NodeAddress serverAddr)
        {
            _serverAddr = serverAddr;
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

        protected abstract Task MessageHandler();
    }

    public class RemoteNodeRaftMessageQueue : RaftMessageQeuue
    {
        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;
        private readonly NodeAddress _clientAddr;

        public RemoteNodeRaftMessageQueue(NodeAddress serverAddr, NodeAddress clientAddr, RpcClient rpcClient)
            : base(serverAddr)
        {
            _serverAddr = serverAddr;
            _clientAddr = clientAddr;
            _rpcClient = rpcClient;

        }
        protected override async Task MessageHandler()
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
    public class LocalNodeRaftMessageQueue : RaftMessageQeuue
    {
        private readonly NodeAddress _serverAddr;
        private readonly NodeAddress _clientAddr;
        private readonly RaftFollower _follower;
        public LocalNodeRaftMessageQueue(NodeAddress serverAddr, NodeAddress clientAddr, RaftFollower follower)
            : base(serverAddr)
        {
            _serverAddr = serverAddr;
            _clientAddr = clientAddr;
            _follower = follower;
        }
        protected override async Task MessageHandler()
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
                        var respMsg = await _follower.DoRequest(raftMsg);
                        reqItem.Response.SetResult(respMsg);
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
        private UInt64 _logIndex = 1;
        private UInt64 _committedLogIndex = 0;
        private UInt64 _committedLogTerm = 0;
        private NodeAddress _candidate;
        private UInt64 _candidateTerm = 0;
        private EntityNote _enityNotebook;
        private IConsensusNotification _notificationSubscriber;

        class CommitingItem
        {
            public UInt64 LogIndex { get; set; }
            public byte[] Data { get; set; }
            public TaskCompletionSource<bool> CommittingTask { get; set; }
        }
        private List<CommitingItem> _pendingCommittingList = new List<CommitingItem>();

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

        public void SubscribeNotification(IConsensusNotification listener)
        {
            _notificationSubscriber = listener;
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
        public UInt64 CandidateTerm => _candidateTerm;
        public NodeAddress CandidateNode => _candidate;

        public Int64 LastCommitedLogIndex
        {
            get; set;
        } = -1;

        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private SemaphoreSlim _commitLock = new SemaphoreSlim(1);

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
            _candidateTerm = candidateTerm;
            _candidate = candidate;
        }
        public void TestSetCommittedLogIndex(UInt64 committedLogIndex)
        {
            _committedLogIndex = committedLogIndex;
        }

        public async Task Commit(UInt64 logIndex, byte[] data)
        {
            var commitTask = new TaskCompletionSource<bool>();
            // pending committed entry
            await _lock.WaitAsync();
            using (var autoLock = new AutoLock(_lock))
            {
                _committedLogIndex = logIndex;
                if (LastCommitedLogIndex < (Int64)_committedLogIndex)
                {
                    await _commitLock.WaitAsync();
                    using (var autoLock2 = new AutoLock(_commitLock))
                    {
                        for (Int64 commitLogIndex = LastCommitedLogIndex + 1; commitLogIndex <= (Int64)_committedLogIndex; commitLogIndex++)
                        {
                            var commitItem = new CommitingItem()
                            {
                                LogIndex = (UInt64)commitLogIndex,
                            };
                            if ((UInt64)commitLogIndex == logIndex)
                            {
                                commitItem.Data = data;
                                commitItem.CommittingTask = commitTask;
                            }
                            else
                            {
                                var logEntity = await _enityNotebook.GetEntityAsync((UInt64)commitLogIndex);
                                commitItem.Data = logEntity != null ? logEntity.Content : null;
                            }
                            _pendingCommittingList.Add(commitItem);
                        }
                    }
                    LastCommitedLogIndex = (Int64)_committedLogIndex;
                }
                else
                {
                    commitTask.SetResult(true);
                }
            }

            var task = Task.Run(async () =>
            {
                do
                {
                    await _commitLock.WaitAsync();
                    using (var autoLock = new AutoLock(_commitLock))
                    {
                        if (_pendingCommittingList.Count == 0)
                        {
                            return;
                        }
                        var committingItem = _pendingCommittingList[0];
                        var entity = await _enityNotebook.GetEntityAsync(committingItem.LogIndex);
                        //  committing entity
                        // TODO: notify subscriber

                        if (_notificationSubscriber != null)
                        {
                            await _notificationSubscriber.UpdateSuccessfullDecree(
                                committingItem.LogIndex, new ConsensusDecree()
                                {
                                    Data = committingItem.Data
                                });
                        }

                        if (committingItem.CommittingTask != null)
                        {
                            committingItem.CommittingTask.SetResult(true);
                        }
                        _pendingCommittingList.RemoveAt(0);
                    }

                } while (true);
            });

            await commitTask.Task;
        }

        private async Task<AppendEntityRespMessage> AppendEntity(AppendEntityReqMessage req)
        {
            // 1. Reply false if term < currentTerm(§5.1)
            // 2.Reply false if log doesn’t contain an entry at prevLogIndex
            // whose term matches prevLogTerm(§5.3)
            // 3.If an existing entry conflicts with a new one(same index
            // but different terms), delete the existing entry and all that
            // follow it(§5.3)
            // 4.Append any new entries not already in the log
            // 5.If leaderCommit > commitIndex, set commitIndex =
            // min(leaderCommit, index of last new entry)


            var resp = new AppendEntityRespMessage();
            resp.Term = _term;
            await _lock.WaitAsync();
            using (var autoLock = new AutoLock(_lock))
            {
                // 1. term < current Term
                if (req.Term < _term)
                {
                    // new leader genertaed
                    resp.Succeed = false;
                    return resp;
                }

                // Impossible situation
                if (req.CommittedLogIndex < _committedLogIndex)
                {
                    resp.Succeed = false;
                    return resp;
                }

                // 2. previous logIndex exist
                bool entityExist = await _enityNotebook.IsEntityExist(req.PreviousLogTerm, req.PreviousLogIndex);
                if (!entityExist)
                {
                    resp.Succeed = false;
                    return resp;
                }

                // 3. If an existing entry conflicts with a new one(same index
                // but different terms), delete the existing entry and all that
                // follow it(§5.3)
                // 4.Append any new entries not already in the log

                // write the entity
                await _enityNotebook.AppendEntity(req.Term, req.LogIndex, req.CommittedLogIndex,
                    req.Data);

                if (req.Term > _term || _leader == null)
                {
                    _term = req.Term;
                    resp.Term = _term;
                    _leader = NodeAddress.DeSerialize(req.SourceNode);
                }

                //5.If leaderCommit > commitIndex,
                if (req.CommittedLogIndex > _committedLogIndex)
                {
                    _committedLogIndex = req.CommittedLogIndex;
                }
                _lastLeaderMsgTime = DateTime.Now;

                // write the latest log
                resp.Succeed = true;
            }
            await Commit(_committedLogIndex, req.Data);

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

            if (req.Term <= _candidateTerm)
            {
                resp.Succeed = false;
                return resp;
            }

            // $5.4, leader election restriction
            //  the voter denies its vote if its own log is more up-to-date than
            //  that of the candidate.
            //       1, 2       1, 2        1, 2, 3       1, 2, 3         1, 2,   3,
            //
            //  S1   1  2       1  2        1  2  4       1  3 (o)        1  2    4
            //  S2   1  2       1  2        1  2          1  3 (o)        1  2    4
            //  S3   1          1           1  2 (r)      1  3 (o)        1  2(r) 4
            //  S4   1          1           1             1  3 (r)        1
            //  S5   1          1  3        1  3          1  3            1  3
            //        a.        b.          c.            d.              e.
            // in c), S1 is selected as leader again, it will replicate log(2)[2] to 
            // all the followers (in this case, it replicated to S3).
            // And after that, it append a new entity log(3).
            // If we commit the log(2)[2], there're could be issue in d).
            // in d), S1 crash again, and S5 is seleted as leader. It will replicate
            // it's entity log(2)[3] to all the follewers. In such scenario, 
            // log(2)[2] will be overwritten. If in c), it's committed, there would be problem
            //
            // if new restriction imported to leader, 
            //   Leader only commit entry by relica count for log entry in current term
            // The issue will be solved.
            // In above case, step c) will not commit log(2)[2], because even it's replciated
            // to majority, it's not current term entry.
            // In order to commit it, it has wto wait for current term entry log(3)[4] to be
            // committed. That need log(3)[4] be replciatd to majority. 
            // in e), log(3)[4] is replicated to majority, and is committed, hense will commit
            // log(2)[2].
            // In such case, even if S1 crash again, and S5 try to be seleted as leader, there
            // wil be no problem. 
            // When S4 try to vote leader, it's term will be 4. It an not win, because majority
            // of nodes already have term 4's entity.
            //
            if (req.LastLogEntryTerm < _term ||
                req.LastLogEntryIndex < _committedLogIndex)
            {
                resp.Succeed = false;
                return resp;
            }


            _candidateTerm = req.Term;
            _candidate = NodeAddress.DeSerialize(req.SourceNode);

            resp.Succeed = true;
            return resp;
        }

    }

    public class RaftLeader
    {
        private readonly RpcClient _rpcClient;
        private readonly NodeAddress _serverAddr;
        private readonly string _serverAddreStr;
        private readonly ConsensusCluster _cluster;

        private ConcurrentDictionary<string, RaftMessageQeuue> _raftMessageList;

        private Task _matainTask = null;
        private DateTime _lastLeaderClaim = DateTime.Now;

        private UInt64 _term = 0;
        private UInt64 _logIndex = 0;
        private UInt64 _prevLogTerm = 0;
        private UInt64 _committedLogIndex = 0;

        private RaftFollower _follwer = null;

        EntityNote _entityNoteBook;
        IConsensusNotification _notificationSubscriber;

        public RaftLeader(
            NodeAddress serverAddress,
            RpcClient rpcClient,
            ConsensusCluster cluster,
            UInt64 term, UInt64 logIndex,
            ConcurrentDictionary<string, RaftMessageQeuue> raftMesssageQueues,
            EntityNote entityNoteBook,
            RaftFollower follower)
        {
            _rpcClient = rpcClient;
            _serverAddr = serverAddress;
            _serverAddreStr = NodeAddress.Serialize(_serverAddr);
            _cluster = cluster;
            _follwer = follower;
            _term = term;
            _logIndex = logIndex;
            _raftMessageList = raftMesssageQueues;
            _entityNoteBook = entityNoteBook;
            _matainTask = Task.Run(async () =>
            {
                await Mantain();
            });
        }
        public void SubscribeNotification(IConsensusNotification listener)
        {
            _notificationSubscriber = listener;
        }

        /// <summary>
        /// For raft, it never commit an entry of previous term, based on majority
        /// have already accepted the appendentity. It only commit entity of current
        /// term if majority accepted the appendentity. And also, if one entity 
        /// is committed, all previous should be committed.
        /// Also, when select a leader, only the node whose log index >= the majority
        /// node's current log index can be selected as leader. 
        /// If we have the above assumption, In raft paper figure 8, in step 3, even if
        /// log index 2 in term 2 is appended to majority, it's not committed, because the
        /// current term (4)'s log index 3 is only appended to 1 nodes. If now leader
        /// crashed, and S5 is selected as leader again, log index 2 of term 3 is replicated
        /// to all other nodes. This would be fine, even if it overwritten index 2 of term 2,
        /// which has already be appended to majoriy nodes. Because the index 2 of term 2 is not
        /// committed.
        /// However, if before S1 crashes, it replicate log index 3 of term 4 to majority nodes
        /// then log index 3 is committed, and log index 2 of term 2 will be comitted 
        /// automatically. This would have no problem, if S1 crashes now and S5 restarted. 
        /// Because now S5 can never be selected as a leader, because one nodes of the majority
        /// would have a log index more late than S5.
        /// </summary>
        /// <param name="decree"></param>
        /// <param name="decreeNo"></param>
        /// <returns></returns>

        public async Task<ProposeResult> Propose(ConsensusDecree decree, ulong decreeNo)
        {
            // if not leader
            // request append

            // TODO: Add race control about the log index allocate
            // 1. get a log index
            // 2. add the log entry into append queue (make sure it's appended in sequence)
            // 3. Add log entryes to other followers' append queue (make sure they are appended in sequence).
            //    step 1 to step 3 need be done in a lock
            // 4. Wait for logs appended in current node & n/2 other folloers appended
            // 5. commit the log. No need to make sure the log committed in sequence.
            // 6. When replay logs, only the log index < commit log index can be replayed.
            //    Suppose the last log index can not be replayed when all logs a read
            //    However, when the leader try to append the uncommitted logs to followers
            //    and new transactions comes and commited the prevous log will be committed.
            //    Here's a question, if no futher transactions comes, the last one will not
            //    be committed, which is not consistent with the previous term.
            //

            var appendRequest = new AppendEntityReqMessage()
            {
                Data = decree != null ? decree.Data : null,
                SourceNode = _serverAddreStr,
                TargetNode = _serverAddreStr
            };

            await AppendRequest(appendRequest);

            return new ProposeResult()
            { };
        }

        public bool IsValid()
        {
            return true;
        }

        public UInt64 Term => _term;

        public UInt64 LogIndex => _logIndex;

        public void TestSetCommitedLogIndex(UInt64 comittedLogIndex)
        {
            _committedLogIndex = comittedLogIndex;
        }

        public void TestSetPrevLogTerm(UInt64 term)
        {
            _prevLogTerm = term;
        }

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

                    await AppendRequest(emptyRequest);

                    await Task.Delay(30000); // delay 30s;
                    continue;
                }

            } while (true);

        }

        private async Task AppendRequest(AppendEntityReqMessage appendRequest)
        {
            UInt64 previousIndex = UInt64.MaxValue;
            UInt64 previousTerm = UInt64.MaxValue;
            Task<RaftMessage> localAppendTask = null;
            lock (this)
            {
                if (_logIndex > 1)
                {
                    previousIndex = _logIndex - 1;
                    previousTerm = _prevLogTerm;
                }


                appendRequest.CommittedLogIndex = _committedLogIndex;
                appendRequest.LogIndex = _logIndex;
                appendRequest.Term = _term;
                appendRequest.PreviousLogIndex = previousIndex;
                appendRequest.PreviousLogTerm = previousTerm;

                _logIndex++;
                _prevLogTerm = _term;

                localAppendTask = _follwer.DoRequest(appendRequest);
            }

            do
            {
                try
                {
                    //await _entityNoteBook.AppendEntity(_term, _logIndex, _committedLogIndex, decree.Data);
                    await AppendRequestToAllFollowers(appendRequest);
                    await localAppendTask;

                    await _follwer.Commit(appendRequest.LogIndex, appendRequest.Data);

                    lock (this)
                    {
                        if (_follwer.CommitedLogIndex > _committedLogIndex)
                        {
                            _committedLogIndex = _follwer.CommitedLogIndex;
                        }
                    }
                    //
                    break;
                }
                catch (Exception e)
                {
                    throw e;
                }
            } while (true);

        }

        public void AddLogToReplicateQeueus(LogEntity log, UInt64 prevTerm, UInt64 committedLogIndex)
        {
            var appendRequest = new AppendEntityReqMessage()
            {
                SourceNode = _serverAddreStr,
                TargetNode = _serverAddreStr,

                Term = log.Term,
                LogIndex = log.LogIndex,

                CommittedLogIndex = committedLogIndex,// committed index;

                PreviousLogTerm = prevTerm, //
                PreviousLogIndex = log.LogIndex,

                Data = log.Content
            };

            foreach (var follower in _cluster.Members)
            {
                if (_serverAddr.Equals(follower))
                {
                    continue;
                }
                var queue = GetMessageQueue(NodeAddress.Serialize(follower));
                var reqItem = new RaftMessageQeuue.RequestItem();
                reqItem.Request = appendRequest.DeepCopy();
                reqItem.Response = new TaskCompletionSource<RaftMessage>();
                queue.AddRaftMessage(reqItem);
            }
        }
        private async Task AppendRequestToAllFollowers(AppendEntityReqMessage append)
        {

            var taskList = new List<Task<RaftMessage>>();
            foreach (var follower in _cluster.Members)
            {
                if (_serverAddr.Equals(follower))
                {
                    continue;
                }
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
                    if (resp.Term != _term)
                    {
                        // new leader generated
                        throw new Exception("new leader");
                    }
                    ++succeedCount;
                    if (succeedCount >= _cluster.Members.Count / 2 )
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
                if (destAddr.Equals(_serverAddr))
                {
                    messageQueue = new LocalNodeRaftMessageQueue(_serverAddr, _serverAddr, _follwer);
                }
                else
                {
                    messageQueue = new RemoteNodeRaftMessageQueue(destAddr, _serverAddr, _rpcClient);

                }
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
                var destAddr = NodeAddress.DeSerialize(targetNode);
                messageQueue = new RemoteNodeRaftMessageQueue(destAddr, _serverAddr, _rpcClient);

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
            //_leader = new RaftLeader(_serverAddr, _rpcClient, _cluster, _term, _logIndex, _raftMessageList, _entityNote);
            EnableMatain = false;
            _matainTask = Task.Run(async () =>
            {
                await Mantain();
            });
        }

        public async ValueTask DisposeAsync()
        {

        }

        bool _notfierCalled = false;
        public async Task Load(DataSource dataSource)
        {
            await _entityNote.Load();
            var logEntities = _entityNote.Test_GetAppendLogEntities();
            var commitedLogIndex = _entityNote.CommittedLogIndex;
            foreach(var logEntity in logEntities)
            {
                if ((Int64)logEntity.Key <= commitedLogIndex)
                {
                    // committed
                    if (_notificationSubscriber == null)
                    {
                        continue;
                    }
                    await _notificationSubscriber.UpdateSuccessfullDecree(
                        logEntity.Key, new ConsensusDecree()
                        {
                            Data = logEntity.Value.Content
                        });
                    _notfierCalled = true;
                }
            }
            _logIndex = (UInt64)_entityNote.MaxLogIndex;
            _follower.LastCommitedLogIndex = commitedLogIndex;
            _term = _entityNote.MaxTerm;
            _follower.TestSetCurrentTerm(_term);
            _follower.TestSetLogIndex(_logIndex);
            _follower.TestSetCommittedLogIndex((UInt64)commitedLogIndex);


            // last one which does not include the committed no,
            // need to wait for new leader which will propose a new
            // entity and commited previous uncommitted logindex

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

        public void SubscribeNotification(IConsensusNotification listener)
        {
            _notificationSubscriber = listener;
            if (_leader != null)
            {
                _leader.SubscribeNotification(listener);
            }
            if (_follower != null)
            {
                _follower.SubscribeNotification(listener);
            }
        }
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
                    _follower.Term,
                    _follower.LogIndex,
                    _raftMessageList);
                try
                {
                    await _candidate.RequestLeaderVoteFromAllFollowers();

                    _leader = new RaftLeader(_serverAddr, _rpcClient, _cluster,
                        _candidate.CurTerm, (UInt64)((Int64)_candidate.LastLogEntryIndex + 1),
                        _raftMessageList, _entityNote, _follower);
                    _leader.TestSetCommitedLogIndex((UInt64)_entityNote.CommittedLogIndex);
                    _leader.TestSetPrevLogTerm(_entityNote.MaxTerm);
                    _leader.SubscribeNotification(_notificationSubscriber);

                    // first replicate all the log not committed
                    UInt64 prevTerm = _entityNote.LastCommittedLogTerm;
                    for (Int64 logIndex = _entityNote.CommittedLogIndex + 1; logIndex <= _entityNote.MaxLogIndex; logIndex++)
                    {
                        var log = await _entityNote.GetEntityAsync((UInt64)logIndex);
                        _leader.AddLogToReplicateQeueus(log, prevTerm, (UInt64)_entityNote.CommittedLogIndex);
                        prevTerm = log.Term;
                    }

                    // submit a empty request, which will commit
                    // all previous request
                    await _leader.Propose(null, 0);

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
