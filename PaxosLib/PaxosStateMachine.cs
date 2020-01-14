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
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.StateMachine
{
    public class StateMachineRequest
    {
        public string RequestId { get; set; }
        public string Content { get; set; }

        public TaskCompletionSource<bool> Result { get; set; }
    }

    class StateMachineRequestSerializer
    {
        public static string Serialize(StateMachineRequest req)
        {
            return req.RequestId + "#" + req.Content;
        }

        public static StateMachineRequest DeSerialize(string content)
        {
            var separatorIndex = content.IndexOf('#');
            return new StateMachineRequest()
            {
                RequestId = content.Substring(0, separatorIndex),
                Content = content.Substring(separatorIndex + 1)
            };
        }
    }

    public class SlideWindow
    {
        public class Item
        {
            public ulong Seq { get; set; }
            public object Content { get; set; }
        }

        SemaphoreSlim _lock = new SemaphoreSlim(1);
        private ulong _baseSeq = 0;
        private ulong _lastReadySeq = 0;
        private SortedList<ulong, Item> _itemList = new SortedList<ulong, Item>();

        public SlideWindow(ulong baseSeq)
        {
            _baseSeq = baseSeq;
            _lastReadySeq = _baseSeq;
        }

        public void Add(ulong seq, object content)
        {
            var item = new Item() { Seq = seq, Content = content };
            lock(_itemList)
            {
                if (!_itemList.ContainsKey(seq))
                {
                    _itemList.Add(seq, item);
                }
            }
        }

        public Item Pop()
        {
            lock (_itemList)
            {
                if (_itemList.Count == 0)
                {
                    return null;
                }

                if (_itemList.Keys[0] == _lastReadySeq + 1)
                {
                    _lastReadySeq += 1;
                    var item = _itemList[_lastReadySeq];
                    _itemList.RemoveAt(0);
                    return item;
                }

                return null;
            }
        }

        public ulong GetPendingSequenceCount()
        {
            lock(_itemList)
            {
                return (ulong)_itemList.Count;
            }
        }

        public Task WaitReadyToContinue()
        {
            if (_itemList.Count < 10)
            {
                return Task.CompletedTask;
            }
            return _lock.WaitAsync();
        }
    }

    public abstract class PaxosStateMachine : IDisposable, IPaxosNotification
    {
        private PaxosNode _node;
        private SlideWindow _reqSlideWindow = new SlideWindow(0);
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

        public async Task Load(string proposerLog, string voterLog)
        {
            Dispose();

            await _node?.Load(proposerLog, voterLog);
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

            await request.Result.Task;
        }

        public virtual Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree)
        {
            var internalRequest = new InternalRequest()
            {
                Result = null,
                SequenceNo = decreeNo,
                Request = StateMachineRequestSerializer.DeSerialize(decree.Content)
            };
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

        public virtual Task Checkpoint()
        {
            return Task.CompletedTask;
        }

        protected abstract Task ExecuteRequest(StateMachineRequest request);

    }
}
