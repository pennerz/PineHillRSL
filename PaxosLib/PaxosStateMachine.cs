using Paxos.Common;
using Paxos.Protocol;
using Paxos.Network;
using Paxos.Node;
using Paxos.Persistence;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Request;
using Paxos.Rpc;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.StateMachine
{
    public class StateMachineRequest
    {
        public string RequestId { get; set; }
        public UInt64 SequenceId { get; set; }
        public string Content { get; set; }

        public TaskCompletionSource<bool> Result { get; set; }
    }

    class StateMachineRequestSerializer
    {
        public static string Serialize(StateMachineRequest req)
        {
            return req.RequestId + "#" + req.SequenceId.ToString() + "#" + req.Content;
        }

        public static StateMachineRequest DeSerialize(string content)
        {
            var separatorIndex = content.IndexOf('#');
            var requestId = content.Substring(0, separatorIndex);
            content = content.Substring(separatorIndex + 1);
            separatorIndex = content.IndexOf('#');
            UInt64 sequenceId = 0;
            UInt64.TryParse(content.Substring(0, separatorIndex), out sequenceId);
            return new StateMachineRequest()
            {
                RequestId = requestId,
                SequenceId = sequenceId,
                Content = content.Substring(separatorIndex + 1)
            };
        }
    }

    public abstract class PaxosStateMachine : IDisposable, IPaxosNotification
    {
        private PaxosNode _node;
        private SlidingWindow _reqSlideWindow = new SlidingWindow(0, null);
        SemaphoreSlim _lock = new SemaphoreSlim(1);
        private Dictionary<string, StateMachineRequest> _requestList = new Dictionary<string, StateMachineRequest>();

        private class InternalRequest
        {
            public TaskCompletionSource<bool> Result { get; set; }
            public ulong SequenceNo { get; set; }
            public StateMachineRequest Request { get; set; }
        }


        public PaxosStateMachine(
            PaxosCluster cluster,
            NodeInfo nodeInfo)
        {
            _node = new PaxosNode(cluster, nodeInfo);
            _node.SubscribeNotification(this);

        }

        public void Dispose()
        {
            _node?.Dispose();
        }

        public Task Load(string metaLog)
        {
            return _node?.Load(metaLog);
        }

        public async Task Request(StateMachineRequest request)
        {
            /*
            while(_reqSlideWindow.GetPendingSequenceCount() > 200)
            {
                await Task.Delay(100);
            }*/
            request.RequestId = Guid.NewGuid().ToString();
            request.Result = new TaskCompletionSource<bool>();
            lock(_requestList)
            {
                _requestList.Add(request.RequestId, request);

            }
            var begin = DateTime.Now;
            var result = await _node.ProposeDecree(new PaxosDecree() { Content = StateMachineRequestSerializer.Serialize(request) }, 0);
            var proposeTime = DateTime.Now - begin;
            if (proposeTime.TotalMilliseconds > 500)
            {
                Console.WriteLine("too slow");
            }
            request.SequenceId = result.DecreeNo;

            await request.Result.Task;
        }

        public virtual async Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree)
        {
            var internalRequest = new InternalRequest()
            {
                Result = null,
                SequenceNo = decreeNo,
                Request = StateMachineRequestSerializer.DeSerialize(decree.Content)
            };
            internalRequest.Request.SequenceId = decreeNo;
            _reqSlideWindow.Add(decreeNo, internalRequest);
            var task = Task.Run(async () =>
            {
                var finishedRequestList = new List<StateMachineRequest>();
                await _lock.WaitAsync();
                using (var sync = new AutoLock(_lock))
                {
                    var item = _reqSlideWindow.Pop();
                    while (item != null)
                    {
                        var originalReq = item.Content as InternalRequest;
                        var stateMachineRequest = originalReq.Request;
                        await ExecuteRequest(stateMachineRequest);

                        lock (_requestList)
                        {
                            if (_requestList.ContainsKey(stateMachineRequest.RequestId))
                            {
                                finishedRequestList.Add(_requestList[stateMachineRequest.RequestId]);
                                _requestList.Remove(stateMachineRequest.RequestId);
                            }
                        }
                        item = _reqSlideWindow.Pop();
                    }
                }
                foreach(var request in finishedRequestList)
                {
                    request.Result?.SetResult(true);
                }
            });
        }

        public Task<UInt64> Checkpoint(Stream checkpointStream)
        {
            return OnCheckpoint(checkpointStream);
        }

        public async Task LoadCheckpoint(UInt64 decreeNo, Stream checkpointStream)
        {
            await OnLoadCheckpoint(decreeNo, checkpointStream);
            var lastDecreeNo = decreeNo;
            if (lastDecreeNo > 0)
            {
                --lastDecreeNo;
            }
            _reqSlideWindow = new SlidingWindow(lastDecreeNo, null);
        }

        protected abstract Task ExecuteRequest(StateMachineRequest request);
        protected abstract Task<UInt64> OnCheckpoint(Stream checkpointStream);
        protected abstract Task OnLoadCheckpoint(UInt64 decreeNo, Stream checkpointStream);

    }
}
