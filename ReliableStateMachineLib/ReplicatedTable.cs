using PineHillRSL.Consensus.Node;
using PineHillRSL.Common;
using PineHillRSL.Network;
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

    public class ReplicatedTable : StateMachine.ReliableStateMachine
    {
        class Row : IComparable<Row>
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
                }
            }

            public string CheckpointingValue { get; set; }

            public int CompareTo(Row rhs)
            {
                return this.Key.CompareTo(rhs.Key);
            }
        }
        //SortedDictionary<string, Row> _table = new SortedDictionary<string, Row>();
        SortedSet<Row> _table = new SortedSet<Row>();

        List<ReplicatedTableRequest> _queueRequests = new List<ReplicatedTableRequest>();
        ConcurrentDictionary<object, BatchRplicatedTableRequest> _pendingRequests = new ConcurrentDictionary<object, BatchRplicatedTableRequest>();
        SemaphoreSlim _tableUpdateLock = new SemaphoreSlim(1);
        UInt64 _lastestSeqNo = 0;

        bool _isCheckpointing = false;
        UInt64 _checkpointSeqNo = 0;

        public ReplicatedTable(
            ConsensusCluster cluster,
            NodeAddress serverAddr) : base(cluster, serverAddr)
        {
            
        }

        public async Task InstertTable(ReplicatedTableRequest tableRequest)
        {
            tableRequest.Result = new TaskCompletionSource<bool>();

            // precondition check
            /*
            Row row = null;
            if (_table.TryGetValue(tableRequest.Key, out row))
            {
                lock(row)
                {
                    // check etag

                }
            }*/


            lock (_queueRequests)
            {
                _queueRequests.Add(tableRequest);
            }
            var task = ProcessRequest();
            await tableRequest.Result.Task;

        }

        public async Task<string> ReadTable(string key)
        {
            var fakeRow = new Row()
            { Key = key};

            Row row;
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                if (_table.TryGetValue(fakeRow, out row))
                {
                    return row.Value;
                }
                /*
                if (_table.ContainsKey(key))
                {
                    return _table[key].Value;
                }*/
                return null;
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
            //bool checkpointing = false;
            //await _tableUpdateLock.WaitAsync();
            //var autoLock = new AutoLock(_tableUpdateLock);
            //using (autoLock)
            //{
            //    checkpointing = _isCheckpointing;
            //}

            var fakeRow = new Row();
            var result = RequestSerializer.DeSerialize(request.Content);
            var batchRequest = result as BatchRplicatedTableRequest;
            if (batchRequest != null)
            {
                var rowList = new List<Row>();
                foreach(var req in batchRequest.Requests)
                {
                    rowList.Add(new Row()
                    {
                        Key = req.Key,
                        Value = req.Value
                    });

                }
                DateTime beforeWait = DateTime.Now;
                await _tableUpdateLock.WaitAsync();
                StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.TableLockWaitTime,
                    (int)(DateTime.Now - beforeWait).TotalMilliseconds);

                var autoLock = new AutoLock(_tableUpdateLock);
                using (autoLock)
                //lock(_tableUpdateLock)
                {
                    for (int i = 0; i < rowList.Count; ++i)
                    {
                        var tableRequst = batchRequest.Requests[i];
                        var row = rowList[i];


                        /*
                        if (_table.ContainsKey(tableRequst.Key))
                        {
                            var row = _table[tableRequst.Key];
                            lock(row)
                            {
                                if (_isCheckpointing && _table[tableRequst.Key].CheckpointingValue == null)
                                {
                                    _table[tableRequst.Key].CheckpointingValue = _table[tableRequst.Key].Value;
                                }
                                _table[tableRequst.Key].Value = tableRequst.Value;
                            }
                        }
                        else
                        {
                            if (!_table.TryAdd(
                                tableRequst.Key,
                                new Row()
                                {
                                    Key = tableRequst.Key,
                                    Value = tableRequst.Value,
                                    CheckpointingValue = ""
                                }))
                            {
                                // should not happen
                            }
                        }*/
                        Row resultRow = null;
                        if (_table.TryGetValue(row, out resultRow))
                        {
                            lock (resultRow)
                            {
                                if (_isCheckpointing && resultRow.CheckpointingValue == null)
                                {
                                    resultRow.CheckpointingValue = resultRow.Value;
                                }
                                resultRow.Value = tableRequst.Value;
                            }
                        }
                        else
                        {
                            _table.Add(row);
                        }
                    }
                    _lastestSeqNo = request.SequenceId;
                }
            }
            else
            {
                var tableRequest = result as ReplicatedTableRequest;
                var row = new Row()
                {
                    Key = tableRequest.Key,
                    Value = tableRequest.Value
                };

                await _tableUpdateLock.WaitAsync();
                var autoLock = new AutoLock(_tableUpdateLock);
                using (autoLock)
                {
                    /*
                    if (_table.ContainsKey(tableRequest.Key))
                    {
                        _table[tableRequest.Key].Value = tableRequest.Value;
                    }
                    else
                    {
                        if (!_table.TryAdd(tableRequest.Key,
                            new Row()
                            {
                                Key = tableRequest.Key,
                                Value = tableRequest.Value,
                                CheckpointingValue = ""
                            }))
                        {
                            // should not happen
                        }
                    }*/
                    Row resultRow = null;
                    if (_table.TryGetValue(row, out resultRow))
                    {
                        lock (resultRow)
                        {
                            if (_isCheckpointing && row.CheckpointingValue == null)
                            {
                                resultRow.CheckpointingValue = resultRow.Value;
                            }
                            resultRow.Value = tableRequest.Value;
                        }
                    }
                    else
                    {
                        _table.Add(row);
                    }

                    _lastestSeqNo = request.SequenceId;
                }
            }
            return ;
        }

        protected override async Task<UInt64> OnCheckpoint(Stream checkpointStream)
        {
            var databuf = new byte[1024 * 4096]; // 4M buffer
            StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointCount, 1);
            var datalen = 0;
            UInt64 checkpointedSeqNo = 0;
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                // TODO: handle previous checkpoint not finish issue
                _isCheckpointing = true;
                _checkpointSeqNo = _lastestSeqNo;
            }

            Row low = null;
            List<Row> copyRows = new List<Row>();
            do
            {
                await _tableUpdateLock.WaitAsync();
                var begin = DateTime.Now;
                autoLock = new AutoLock(_tableUpdateLock);
                using (autoLock)
                {
                    SortedSet<Row> copyView = null;
                    if (low == null)
                    {
                        copyView = _table;
                    }
                    else
                    {
                        copyView = _table.GetViewBetween(low, _table.Max);
                    }

                    if (copyView.Count == 0)
                    {
                        break;
                    }
                    if (copyView.Count == 1 && copyView.Contains(low))
                    {
                        break;
                    }

                    foreach (var row in copyView)
                    {
                        if (low != null && row.Key.Equals(low.Key))
                        {
                            continue;
                        }
                        low = row;
                        copyRows.Add(row);
                        if (copyRows.Count > 100 * 1024)
                        {
                            break;
                        }
                    }
                    StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime,
                        (int)(DateTime.Now - begin).TotalMilliseconds);
                }

                // write the rows
                int recordLen = 0;
                foreach (var row in copyRows)
                {
                    string rowVal = null;
                    //await _tableUpdateLock.WaitAsync();
                    //autoLock = new AutoLock(_tableUpdateLock);
                    //using (autoLock)
                    lock (row)
                    {
                        if (row.CheckpointingValue != null)
                        {
                            rowVal = row.CheckpointingValue;
                        }
                        else
                        {
                            rowVal = row.Value;
                        }
                        // reset checkpoint value
                        row.CheckpointingValue = null;
                    }
                    if (rowVal.Equals(""))
                    {
                        continue;
                    }

                    while (!SerializeRow(row.Key, rowVal, databuf, datalen, out recordLen))
                    {
                        if (datalen == 0)
                        {
                            // buffer not enough for one row
                            databuf = new byte[databuf.Length * 2];
                        }
                        else
                        {
                            await checkpointStream.WriteAsync(databuf, 0, datalen);
                            StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointDataSize,
                                datalen);
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

                copyRows.Clear();

            } while (true);
            /*
            foreach(var item in _table)
            {
                int recordLen = 0;
                var row = item.Value;
                string rowVal = null;
                //await _tableUpdateLock.WaitAsync();
                //autoLock = new AutoLock(_tableUpdateLock);
                //using (autoLock)
                //lock(row)
                {
                    if  (row.CheckpointingValue != null)
                    {
                        rowVal = row.CheckpointingValue;
                    }
                    else
                    {
                        rowVal = row.Value;
                    }
                }
                if (rowVal.Equals(""))
                {
                    continue;
                }

                while(!SerializeRow(row.Key, rowVal, databuf, datalen, out recordLen))
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
            }*/

            if (datalen > 0)
            {
                await checkpointStream.WriteAsync(databuf, 0, datalen);
                StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointDataSize,
                    datalen);
            }
            checkpointedSeqNo = _checkpointSeqNo;
            await _tableUpdateLock.WaitAsync();
            autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                _isCheckpointing = false;
                _checkpointSeqNo = 0;
            }

            return checkpointedSeqNo;

        }
        protected  async Task<UInt64> OnCheckpoint1(Stream checkpointStream)
        {
            var databuf = new byte[1024 * 4096]; // 4M buffer
            StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointCount, 1);
            var datalen = 0;
            UInt64 checkpointedSeqNo = 0;
            Row low = null;
            List<Row> copyRows = new List<Row>();
            await _tableUpdateLock.WaitAsync();
            var autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                // TODO: handle previous checkpoint not finish issue
                _isCheckpointing = true;
                _checkpointSeqNo = _lastestSeqNo;
                checkpointedSeqNo = _checkpointSeqNo;
                var begin = DateTime.Now;
                autoLock = new AutoLock(_tableUpdateLock);
                using (autoLock)
                {
                    do
                    {
                        SortedSet<Row> copyView = null;
                        if (low == null)
                        {
                            copyView = _table;
                        }
                        else
                        {
                            copyView = _table.GetViewBetween(low, _table.Max);
                        }

                        if (copyView.Count == 0)
                        {
                            break;
                        }
                        if (copyView.Count == 1 && copyView.Contains(low))
                        {
                            break;
                        }

                        foreach (var row in copyView)
                        {
                            if (low != null && row.Key.Equals(low.Key))
                            {
                                continue;
                            }
                            low = row;
                            copyRows.Add(row);
                            if (copyRows.Count > 100 * 1024)
                            {
                                break;
                            }
                        }

                    } while (false);
                }
                StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime,
                    (int)(DateTime.Now - begin).TotalMilliseconds);
            }

            // write the rows
            int recordLen = 0;
            foreach (var row in copyRows)
            {
                string rowVal = null;
                //await _tableUpdateLock.WaitAsync();
                //autoLock = new AutoLock(_tableUpdateLock);
                //using (autoLock)
                lock (row)
                {
                    if (row.CheckpointingValue != null)
                    {
                        rowVal = row.CheckpointingValue;
                    }
                    else
                    {
                        rowVal = row.Value;
                    }
                    // reset checkpoint value
                    row.CheckpointingValue = null;
                }
                if (rowVal.Equals(""))
                {
                    continue;
                }

                while (!SerializeRow(row.Key, rowVal, databuf, datalen, out recordLen))
                {
                    if (datalen == 0)
                    {
                        // buffer not enough for one row
                        databuf = new byte[databuf.Length * 2];
                    }
                    else
                    {
                        await checkpointStream.WriteAsync(databuf, 0, datalen);
                        StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointDataSize,
                            datalen);
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
            /*
            copyRows.Clear();



            do
            {
                await _tableUpdateLock.WaitAsync();
            } while (true);*/
            /*
            foreach(var item in _table)
            {
                int recordLen = 0;
                var row = item.Value;
                string rowVal = null;
                //await _tableUpdateLock.WaitAsync();
                //autoLock = new AutoLock(_tableUpdateLock);
                //using (autoLock)
                //lock(row)
                {
                    if  (row.CheckpointingValue != null)
                    {
                        rowVal = row.CheckpointingValue;
                    }
                    else
                    {
                        rowVal = row.Value;
                    }
                }
                if (rowVal.Equals(""))
                {
                    continue;
                }

                while(!SerializeRow(row.Key, rowVal, databuf, datalen, out recordLen))
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
            }*/

            if (datalen > 0)
            {
                await checkpointStream.WriteAsync(databuf, 0, datalen);
                StatisticCounter.ReportCounter((int)PerCounter.NetworkPerfCounterType.CheckpointDataSize,
                    datalen);
            }
            await _tableUpdateLock.WaitAsync();
            autoLock = new AutoLock(_tableUpdateLock);
            using (autoLock)
            {
                _isCheckpointing = false;
                _checkpointSeqNo = 0;
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
                        var row = new Row()
                        {
                            Key = key,
                            Value = val
                        };

                        Row result = null;
                        if (_table.TryGetValue(row, out result))
                        {
                            lock(result)
                            {
                                result.Value = val;
                            }
                        }
                        else
                        {
                            _table.Add(row);
                        }
                        /*
                        if (_table.ContainsKey(key))
                        {
                            _table[key].Value = val;
                        }
                        else
                        {
                            _table.TryAdd(key,
                                new Row()
                                {
                                    Key = key,
                                    Value = val
                                });
                        }*/
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
