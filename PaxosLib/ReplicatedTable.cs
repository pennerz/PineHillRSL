using PineRSL.Common;
using PineRSL.Network;
using PineRSL.Paxos.Message;
using PineRSL.Paxos.Node;
using PineRSL.Paxos.Notebook;
using PineRSL.Paxos.Persistence;
using PineRSL.Paxos.Protocol;
using PineRSL.Paxos.Request;
using PineRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PineRSL.ReplicatedTable
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
        List<ReplicatedTableRequest> _queueRequests = new List<ReplicatedTableRequest>();
        ConcurrentDictionary<object, BatchRplicatedTableRequest> _pendingRequests = new ConcurrentDictionary<object, BatchRplicatedTableRequest>();
        SemaphoreSlim _tableUpdateLock = new SemaphoreSlim(1);
        UInt64 _lastestSeqNo = 0;

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

        public async Task<string> ReadTable(string key)
        {
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                if (_table.ContainsKey(key))
                {
                    return _table[key];
                }
                else
                {
                    return null;
                }
            }
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

        protected override async Task ExecuteRequest(StateMachine.StateMachineRequest request)
        {
            if (request == null)
            {
                return;
            }
            var result = RequestSerializer.DeSerialize(request.Content);
            var batchRequest = result as BatchRplicatedTableRequest;
            if (batchRequest != null)
            {
                await _tableUpdateLock.WaitAsync();
                var autoLock = new AutoLock(_tableUpdateLock);
                using (autoLock)
                {
                    foreach (var tableRequst in batchRequest.Requests)
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
                    _lastestSeqNo = request.SequenceId;
                }
            }
            else
            {
                var tableRequest = result as ReplicatedTableRequest;
                if (tableRequest != null)
                {
                    lock (_tableUpdateLock)
                    {
                        if (_table.ContainsKey(tableRequest.Key))
                        {
                            _table[tableRequest.Key] = tableRequest.Value;
                        }
                        else
                        {
                            _table.Add(tableRequest.Key, tableRequest.Value);
                        }
                        _lastestSeqNo = request.SequenceId;
                    }
                }
            }
            return ;
        }

        protected override async Task<UInt64> OnCheckpoint(Stream checkpointStream)
        {
            var databuf = new byte[1024 * 4096]; // 4M buffer
            UInt64 checkpointedSeqNo = 0;
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                foreach(var row in _table)
                {
                    int datalen = 0;
                    while(!SerializeRow(row.Key, row.Value, databuf, out datalen))
                    {
                        databuf = new byte[databuf.Length * 2];
                    }
                    await checkpointStream.WriteAsync(databuf, 0, datalen);
                }

                checkpointedSeqNo = _lastestSeqNo;
            }
            return checkpointedSeqNo;
        }

        protected override async Task OnLoadCheckpoint(UInt64 decreeNo, Stream checkpointStream)
        {
            var databuf = new byte[1024 * 4096]; // 4M buffer
            UInt64 checkpointedSeqNo = 0;
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                do
                {
                    var readlen = await checkpointStream.ReadAsync(databuf, 0, sizeof(int));
                    if (readlen != sizeof(int))
                    {
                        break;
                    }
                    var dataSize = BitConverter.ToInt32(databuf, 0);
                    if (dataSize < 0)
                    {
                        break;
                    }
                    readlen = await checkpointStream.ReadAsync(databuf, sizeof(int), dataSize);
                    if (readlen != dataSize)
                    {
                        break;
                    }
                    string key;
                    string val;
                    if (DeSerializeRow(databuf, readlen + sizeof(int), out key, out val))
                    {
                        _table.Add(key, val);
                    }

                } while (true);

                _lastestSeqNo = decreeNo;
            }
        }


        private static bool SerializeRow(string key, string val, byte[] databuf, out int len)
        {
            // recordSizexxxx#xxxx
            var separatorData = Encoding.UTF8.GetBytes("#");
            var keyData = Encoding.UTF8.GetBytes(key);
            var valData = Encoding.UTF8.GetBytes(val);
            int recordSize = separatorData.Length + keyData.Length + valData.Length;
            var sizeData = BitConverter.GetBytes(recordSize);
            if (databuf.Length < recordSize + sizeData.Length)
            {
                len = 0;
                return false;
            }
            int sizeInBuf = 0;
            System.Buffer.BlockCopy(sizeData, 0, databuf, 0, sizeData.Length);
            sizeInBuf += sizeData.Length;
            System.Buffer.BlockCopy(keyData, 0, databuf, sizeInBuf, keyData.Length);
            sizeInBuf += keyData.Length;
            System.Buffer.BlockCopy(separatorData, 0, databuf, sizeInBuf, separatorData.Length);
            sizeInBuf += separatorData.Length;
            System.Buffer.BlockCopy(valData, 0, databuf, sizeInBuf, valData.Length);
            sizeInBuf += valData.Length;
            len = sizeInBuf;
            return true;
        }
        private static bool DeSerializeRow(byte[] databuf, int len, out string key, out string val)
        {
            // recordSizexxxx#xxxx
            key = "";
            val = "";
            string content = Encoding.UTF8.GetString(databuf, sizeof(int), len - sizeof(int));
            var separatorIndex = content.IndexOf("#");
            if (separatorIndex != -1)
            {
                key = content.Substring(0, separatorIndex);
                val = content.Substring(separatorIndex + 1, content.Length - separatorIndex - 1);
            }
            return true;
        }
    }
}
