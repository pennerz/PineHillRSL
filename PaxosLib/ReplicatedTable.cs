using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Message;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Paxos.Notebook;
using PineHillRSL.Paxos.Persistence;
using PineHillRSL.Paxos.Protocol;
using PineHillRSL.Paxos.Request;
using PineHillRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.ReplicatedTable
{
    public class ReplicatedTableRequest
    {
        public string Key { get; set; }
        public string Value { get; set; }

        public DateTime CreatedTime { get; set; } = DateTime.Now;

        public DateTime BeginProcessTime { get; set; } = DateTime.MaxValue;

        public DateTime FinishProcessTime { get; set; } = DateTime.MaxValue;

        public TaskCompletionSource<bool> Result { get; set; }

    }

    public class BatchRplicatedTableRequest
    {
        List<ReplicatedTableRequest> _batchRequest = new List<ReplicatedTableRequest>();
        public List<ReplicatedTableRequest> Requests => _batchRequest;

        public DateTime CreatedTime { get; set; } = DateTime.Now;

        public DateTime BeginProcessTime { get; set; } = DateTime.MaxValue;

        public DateTime FinishTime { get; set; } = DateTime.MaxValue;
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
        class Row
        {
            private string _value;
            public string Key { get; set; }
            public string Value
            {
                get
                {
                    return _value;
                }
                set
                {
                    _value = value;
                    Checkpointed = false;
                }
            }
            public bool Checkpointed { get; set; } = false;
        }
        Dictionary<string, Row> _table = new Dictionary<string, Row>();
        List<ReplicatedTableRequest> _queueRequests = new List<ReplicatedTableRequest>();
        ConcurrentDictionary<object, BatchRplicatedTableRequest> _pendingRequests = new ConcurrentDictionary<object, BatchRplicatedTableRequest>();
        SemaphoreSlim _tableUpdateLock = new SemaphoreSlim(1);
        UInt64 _lastestSeqNo = 0;

        public ReplicatedTable(
            PaxosCluster cluster,
            NodeAddress serverAddr) : base(cluster, serverAddr)
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
                    return _table[key].Value;
                }
                else
                {
                    return null;
                }
            }
        }

        protected Task ProcessRequest()
        {
            if (_pendingRequests.Count > 10)
            {
                Console.WriteLine($"Pending requests: {_pendingRequests.Count}");
            }
            if (_pendingRequests.Count > 100)
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
                            batchRequest.Requests.AddRange(_queueRequests);
                            _queueRequests.Clear();
                        }
                    }
                    if (batchRequest.Requests.Count > 0)
                    {
                        _pendingRequests.TryAdd(batchRequest, batchRequest);
                    }
                    else
                    {
                        return;
                    }
                    BatchRplicatedTableRequest outBatchRequest = null;
                    var request = new StateMachine.StateMachineRequest();
                    request.Content = RequestSerializer.Serialize(batchRequest);
                    await Request(request);
                    _pendingRequests.TryRemove(batchRequest, out outBatchRequest);
                    batchRequest.FinishTime = DateTime.Now;
                    foreach (var req in batchRequest.Requests)
                    {
                        var task = Task.Run(() =>
                        {
                            req.BeginProcessTime = batchRequest.CreatedTime;
                            req.FinishProcessTime = DateTime.Now;
                            req.Result.SetResult(true);
                        });
                    }
                }
            });
            return task;
        }

        protected override async Task ExecuteRequest(StateMachine.StateMachineRequest request)
        {
            if (request == null || string.IsNullOrEmpty(request.Content))
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
                            _table[tableRequst.Key].Value = tableRequst.Value;
                        }
                        else
                        {
                            _table.Add(tableRequst.Key,
                                new Row()
                                {
                                    Key = tableRequst.Key,
                                    Value = tableRequst.Value
                                });
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
                            _table[tableRequest.Key].Value = tableRequest.Value;
                        }
                        else
                        {
                            _table.Add(tableRequest.Key,
                                new Row()
                                {
                                    Key = tableRequest.Key,
                                    Value = tableRequest.Value
                                });
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
            var datalen = 0;
            UInt64 checkpointedSeqNo = 0;
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                foreach(var item in _table)
                {
                    int recordLen = 0;
                    var row = item.Value;
                    if (row.Checkpointed)
                    {
                        continue;
                    }

                    while(!SerializeRow(row.Key, row.Value, databuf, datalen, out recordLen))
                    {
                        if (datalen == 0)
                        {
                            // buffer not enough for one row
                            databuf = new byte[databuf.Length * 2];
                        }
                        else
                        {
                            await checkpointStream.WriteAsync(databuf, 0, datalen);
                            datalen = 0;
                            break;
                        }
                    }
                    datalen += recordLen;
                    if (datalen > databuf.Length)
                    {
                        Console.WriteLine("Fatal error");
                    }
                }
                if (datalen > 0)
                {
                    await checkpointStream.WriteAsync(databuf, 0, datalen);
                }

                checkpointedSeqNo = _lastestSeqNo;
            }
            return checkpointedSeqNo;
        }

        protected override async Task OnLoadCheckpoint(UInt64 decreeNo, Stream checkpointStream)
        {
            var databuf = new byte[1024 * 4096]; // 4M buffer
            //UInt64 checkpointedSeqNo = 0;
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
                        if (_table.ContainsKey(key))
                        {
                            _table[key].Value = val;
                        }
                        else
                        {
                            _table.Add(key,
                                new Row()
                                {
                                    Key = key,
                                    Value = val
                                });
                        }
                    }

                } while (true);

                _lastestSeqNo = decreeNo;
            }
        }


        private static bool SerializeRow(string key, string val, byte[] databuf, int dataOff, out int len)
        {
            // recordSizexxxx#xxxx
            var separatorData = Encoding.UTF8.GetBytes("#");
            var keyData = Encoding.UTF8.GetBytes(key);
            var valData = Encoding.UTF8.GetBytes(val);
            int recordSize = separatorData.Length + keyData.Length + valData.Length;
            var sizeData = BitConverter.GetBytes(recordSize);
            if (databuf.Length < dataOff + recordSize + sizeData.Length)
            {
                len = 0;
                return false;
            }
            int sizeInBuf = dataOff;
            System.Buffer.BlockCopy(sizeData, 0, databuf, sizeInBuf, sizeData.Length);
            sizeInBuf += sizeData.Length;
            System.Buffer.BlockCopy(keyData, 0, databuf, sizeInBuf, keyData.Length);
            sizeInBuf += keyData.Length;
            System.Buffer.BlockCopy(separatorData, 0, databuf, sizeInBuf, separatorData.Length);
            sizeInBuf += separatorData.Length;
            System.Buffer.BlockCopy(valData, 0, databuf, sizeInBuf, valData.Length);
            sizeInBuf += valData.Length;
            len = sizeInBuf - dataOff;
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
