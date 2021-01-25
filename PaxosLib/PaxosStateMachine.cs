using PineHillRSL.Common;
using PineHillRSL.Paxos.Protocol;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Paxos.Request;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.StateMachine
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
            if (string.IsNullOrEmpty(content))
            {
                return null;
            }
            var separatorIndex = content.IndexOf('#');
            if (separatorIndex == -1)
            {
                return null;
            }
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
        private PaxosCluster _cluster;
        private NodeAddress _serverAddr;
        private string _metaLog;
        private SlidingWindow _reqSlideWindow = new SlidingWindow(0, null);
        SemaphoreSlim _lock = new SemaphoreSlim(1);
        private Dictionary<string, StateMachineRequest> _requestList = new Dictionary<string, StateMachineRequest>();

        private Task _matainTask;
        private CancellationTokenSource _exit = new CancellationTokenSource();

        private class InternalRequest
        {
            public TaskCompletionSource<bool> Result { get; set; }
            public ulong SequenceNo { get; set; }
            public StateMachineRequest Request { get; set; }
        }


        public PaxosStateMachine(
            PaxosCluster cluster,
            NodeAddress serverAddr)
        {
            _cluster = cluster;
            _serverAddr = serverAddr;
            _node = new PaxosNode(cluster, serverAddr);
            _node.SubscribeNotification(this);

            _reqSlideWindow = new SlidingWindow(_node.MaxCommittedNo, null);

            MissedRequestTimeoutInSecond = 5 * 60;

            _matainTask = Task.Run(Mantain);

        }

        public void Dispose()
        {
            _exit.Cancel();
            _matainTask?.Wait();
            _node?.Dispose();
            _exit.Dispose();
        }

        public async Task Load(string metaLog)
        {
            _metaLog = metaLog;
            await _node?.Load(metaLog);
            _reqSlideWindow = new SlidingWindow(_node.MaxCommittedNo, null);
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

            await ReqeustInternal(request);
        }

        public virtual Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree)
        {
            var internalRequest = new InternalRequest()
            {
                Result = null,
                SequenceNo = decreeNo,
                Request = StateMachineRequestSerializer.DeSerialize(decree.Content)
            };
            if (internalRequest.Request == null)
            {
                internalRequest.Request = new StateMachineRequest();
                internalRequest.Request.SequenceId = decreeNo;
                internalRequest.Request.Content = "";
            }
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

            return Task.CompletedTask;
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
                //lastDecreeNo;
            }
            _reqSlideWindow = new SlidingWindow(lastDecreeNo, null);
        }

        public int MissedRequestTimeoutInSecond { get; set; }

        private async Task ReqeustInternal(StateMachineRequest request)
        {
            var begin = DateTime.Now;
            do
            {
                try
                {
                    var result = await _node.ProposeDecree(new PaxosDecree() { Content = StateMachineRequestSerializer.Serialize(request) }, 0);
                    var proposeTime = DateTime.Now - begin;
                    if (proposeTime.TotalMilliseconds > 500)
                    {
                        Console.WriteLine("too slow");
                    }
                    request.SequenceId = result.DecreeNo;
                    break;
                }
                catch (Exception /*e*/)
                {
                }

                //
                _node.Dispose();

                _node = new PaxosNode(_cluster, _serverAddr);
                _node.SubscribeNotification(this);

                if (string.IsNullOrEmpty(_metaLog))
                {
                    var instanceName = NodeAddress.Serialize(_serverAddr);
                    _metaLog = ".\\storage\\" + instanceName + ".meta";
                }
                await Task.Run(async () =>
                {
                    await _node.Load(_metaLog, ProposerRole.DataSource.Cluster);
                });

            } while (true);

            await request.Result.Task;

        }

        private async Task Mantain()
        {
            while(true)
            {
                try
                {
                    await Task.Delay(60 * 1000, _exit.Token);
                }
                catch(TaskCanceledException)
                {
                    return;
                }

                DateTime now = DateTime.Now;
                var popElaspedTime = now - _reqSlideWindow.LastPopTime;
                if (popElaspedTime.TotalSeconds > MissedRequestTimeoutInSecond && _reqSlideWindow.Count > 0)
                {
                    // request all the pending request
                    var smallestSeq = _reqSlideWindow.SmallestSeqInWindow;
                    var lastPopSeq = _reqSlideWindow.LastPopSeq;

                    for (ulong missedSeq = lastPopSeq + 1; missedSeq < smallestSeq; missedSeq++)
                    {
                        var fakeRequest = new StateMachineRequest()
                        {
                            RequestId = Guid.NewGuid().ToString(),
                            SequenceId = missedSeq,
                            Content = ""
                        };

                        await ReqeustInternal(fakeRequest);
                    }
                }

            }

        }

        protected abstract Task ExecuteRequest(StateMachineRequest request);
        protected abstract Task<UInt64> OnCheckpoint(Stream checkpointStream);
        protected abstract Task OnLoadCheckpoint(UInt64 decreeNo, Stream checkpointStream);

    }
}
