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
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.ReplicatedTable
{
    public class ReplicatedTableRequest
    {
        public string Key { get; set; }
        public string Value { get; set; }

        public TaskCompletionSource<bool> Result { get; set; }

    }

    public class BatchRplicatedTableRequest
    {
        List<ReplicatedTableRequest> _batchRequest = new List<ReplicatedTableRequest>();
        public List<ReplicatedTableRequest> Requests => _batchRequest;
    }

    class RequestSerializer
    {
        public static string Serialize(ReplicatedTableRequest req)
        {
            return "Single@" + req.Key + "@" + req.Value;
        }

        public static string Serialize(BatchRplicatedTableRequest batchRequest)
        {
            var content = "Batch@";
            foreach(var req in batchRequest.Requests)
            {
                content += "Request:" + req.Key + "@" + req.Value;
            }

            return content;
        }

        public static object DeSerialize(string content)
        {
            var separatorIndex = content.IndexOf('@');
            var type = content.Substring(0, separatorIndex);
            if (type == "Single")
            {
                content = content.Substring(separatorIndex + 1);
                separatorIndex = content.IndexOf('@');
                return new ReplicatedTableRequest()
                {
                    Key = content.Substring(0, separatorIndex),
                    Value = content.Substring(separatorIndex + 1)
                };
            }
            else if (type == "Batch")
            {
                content = content.Substring(separatorIndex + 1);
                var batchRequest = new BatchRplicatedTableRequest();
                do
                {
                    separatorIndex = content.IndexOf("Request:");
                    if (separatorIndex < 0)
                    {
                        break;
                    }
                    content = content.Substring(8);
                    separatorIndex = content.IndexOf("Request:");
                    string requestContent;
                    if (separatorIndex < 0)
                    {
                        requestContent = content;
                        content = "";
                    }
                    else
                    {
                        requestContent = content.Substring(0, separatorIndex);
                        content = content.Substring(separatorIndex);
                    }
                    separatorIndex = requestContent.IndexOf('@');
                    var request = new ReplicatedTableRequest()
                    {
                        Key = requestContent.Substring(0, separatorIndex),
                        Value = requestContent.Substring(separatorIndex + 1)
                    };
                    batchRequest.Requests.Add(request);
                } while (true);
                return batchRequest;
            }

            return null;
        }
    }

    public class ReplicatedTable : StateMachine.PaxosStateMachine
    {
        Dictionary<string, string> _table = new Dictionary<string, string>();
        int _pendingCount = 0;
        List<ReplicatedTableRequest> _queueRequests = new List<ReplicatedTableRequest>();
        ConcurrentDictionary<object, BatchRplicatedTableRequest> _pendingRequests = new ConcurrentDictionary<object, BatchRplicatedTableRequest>();

        public ReplicatedTable(
            PaxosCluster cluster,
            NodeInfo nodeInfo) : base(cluster, nodeInfo)
        {
            
        }

        public async Task InstertTable(ReplicatedTableRequest tableRequest)
        {
            tableRequest.Result = new TaskCompletionSource<bool>();
            lock(_queueRequests)
            {
                _queueRequests.Add(tableRequest);
            }
            var task = ProcessRequest();
            await tableRequest.Result.Task;

        }

        protected Task ProcessRequest()
        {
            if (_pendingRequests.Count > 200)
            {
                return Task.CompletedTask;
            }

            var task = Task.Run(async () =>
            {
                while(true)
                {
                    var batchRequest = new BatchRplicatedTableRequest();
                    lock (_queueRequests)
                    {
                        if (_queueRequests.Count > 500 || _pendingRequests.Count == 0)
                        {
                            foreach (var req in _queueRequests)
                            {
                                batchRequest.Requests.Add(req);
                            }
                            _queueRequests.Clear();
                            _pendingRequests.TryAdd(batchRequest, batchRequest);
                        }
                    }
                    if (batchRequest.Requests.Count == 0)
                    {
                        return;
                    }
                    var request = new StateMachine.StateMachineRequest();
                    request.Content = RequestSerializer.Serialize(batchRequest);
                    await Request(request);
                    foreach (var req in batchRequest.Requests)
                    {
                        req.Result.SetResult(true);
                    }
                    BatchRplicatedTableRequest outBatchRequest = null;
                    _pendingRequests.TryRemove(batchRequest, out outBatchRequest);

                }
            });
            return task;
        }

        protected override Task ExecuteRequest(StateMachine.StateMachineRequest request)
        {
            var result = RequestSerializer.DeSerialize(request.Content);
            var batchRequest = result as BatchRplicatedTableRequest;
            if (batchRequest != null)
            {
                foreach(var tableRequst in batchRequest.Requests)
                {
                    if (_table.ContainsKey(tableRequst.Key))
                    {
                        _table[tableRequst.Key] = tableRequst.Value;
                    }
                    else
                    {
                        _table.Add(tableRequst.Key, tableRequst.Value);
                    }
                }
            }
            else
            {
                var tableRequest = result as ReplicatedTableRequest;
                if (tableRequest != null)
                {
                    if (_table.ContainsKey(tableRequest.Key))
                    {
                        _table[tableRequest.Key] = tableRequest.Value;
                    }
                    else
                    {
                        _table.Add(tableRequest.Key, tableRequest.Value);
                    }

                }
            }
            return Task.CompletedTask;
        }

    }
}
