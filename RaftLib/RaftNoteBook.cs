using PineHillRSL.Consensus.Request;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.Raft.Notebook
{

    public class EntityRecord
    {
        private byte[] _magic = new byte[] { (byte)'E', (byte)'n', (byte)'t', (byte)'y' };
        private UInt64 _term = 0;
        private UInt64 _logIndex = 0;
        private UInt64 _committedLogIndex = 0;
        private byte[] _content;

        public EntityRecord(UInt64 term, UInt64 logIndex, UInt64 committedLogIndex, byte[] decreeContent)
        {
            _term = term;
            _logIndex = logIndex;
            _committedLogIndex = committedLogIndex;
            _content = decreeContent;
        }

        public UInt64 Term => _term;

        public UInt64 LogIndex => _logIndex;

        public UInt64 CommittedLogIndex => _committedLogIndex;

        public byte[] DecreeContent => _content;

        public byte[] Serialize()
        {
            var decreeContentData = _content; // Encoding.UTF8.GetBytes(_decreeContent);
            var termData = BitConverter.GetBytes(_term);
            var logIndexData = BitConverter.GetBytes(_logIndex);
            var committedLogIndexData = BitConverter.GetBytes(_committedLogIndex);

            int dataLength = decreeContentData != null ? decreeContentData.Length : 0;
            int recrodSize = _magic.Length + dataLength
                + termData.Length + logIndexData.Length + committedLogIndexData.Length;
            var buf = new LogBuffer();
            buf.AllocateBuffer(recrodSize + sizeof(int));
            buf.EnQueueData(BitConverter.GetBytes(recrodSize));
            buf.EnQueueData(_magic);
            buf.EnQueueData(termData);
            buf.EnQueueData(logIndexData);
            buf.EnQueueData(committedLogIndexData);
            if (dataLength > 0)
            {
                buf.EnQueueData(decreeContentData);
            }
            return buf.DataBuf;
        }

        public void DeSerialize(byte[] buf, int len)
        {
            int offInBuf = 0;
            var recordSize = BitConverter.ToInt32(buf, 0);
            if (recordSize != len - sizeof(int))
            {
                return;
            }
            offInBuf += sizeof(int);
            var magic = new byte[_magic.Length];
            Array.Copy(buf, offInBuf, magic, 0, magic.Length);
            _magic.SequenceEqual(magic);
            offInBuf += _magic.Length;
            _term = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _logIndex = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _committedLogIndex = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _content = null;
            if (len - offInBuf > 0)
            {
                _content = new byte[len - offInBuf];
                Buffer.BlockCopy(buf, offInBuf, _content, 0, _content.Length);
            }
            //_decreeContent = Encoding.UTF8.GetString(buf, sizeof(UInt64) + sizeof(int), len - sizeof(UInt64) - sizeof(int));
        }

    }

    public class LogEntity
    {
        public UInt64 Term { get; set; }
        public UInt64 LogIndex { get; set; }
        public AppendPosition LogPosition { get; set; }
        public byte[] Content { get; set; }
        public bool IsContentEqual(LogEntity rhs)
        {
            if (rhs == null)
            {
                return false;
            }
            return Term == rhs.Term && LogIndex == rhs.LogIndex;
        }

        public bool IsContentEqual(UInt64 term, UInt64 logIndex)
        {
            return Term == term && LogIndex == logIndex;
        }
    }

    public class EntityNote : IAsyncDisposable
    {
        // persistent module
        private readonly ILogger _metaLogger;
        private readonly ILogger _logger;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);


        private List<Tuple<UInt64, AppendPosition>> _checkpointPositionList = new List<Tuple<UInt64, AppendPosition>>();
        private SortedDictionary<UInt64, LogEntity> _logEntities = new SortedDictionary<ulong, LogEntity>();

        private AppendPosition _lastAppendPosition;
        private UInt64 _maxCommittedLogIndex = 0;
        private UInt64 _maxCommittedLogTerm = 0;
        private UInt64 _maxLogIndex = 0;
        private UInt64 _maxTerm = 0;
        private MetaNote _metaNote = null;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

        private bool _isStop = false;
        private Task _positionCheckpointTask;

        public EntityNote(ILogger entityLogger, ILogger metaLogger = null)
        {
            _logger = entityLogger;
            _metaLogger = metaLogger;
            if (_metaLogger != null)
            {
                _metaNote = new MetaNote(_metaLogger);
            }

            _positionCheckpointTask = Task.Run(async () =>
            {
                do
                {
                    try
                    {
                        await Task.Delay(100000, _cancel.Token);
                    }
                    catch(TaskCanceledException)
                    {
                        return;
                    }

                    UInt64 maxLogEntryIndexNo = 0;
                    AppendPosition lastAppendPosition;
                    lock (_logger)
                    {
                        maxLogEntryIndexNo = _maxLogIndex;
                        lastAppendPosition = _lastAppendPosition;
                    }

                    if (lastAppendPosition != null)
                    {
                        lock (_checkpointPositionList)
                        {
                            _checkpointPositionList.Add(new Tuple<UInt64, AppendPosition>(maxLogEntryIndexNo, lastAppendPosition));
                        }
                    }

                } while (!_isStop);

            });
        }

        public async ValueTask DisposeAsync()
        {
            _isStop = true;
            _cancel.Cancel();
            await _positionCheckpointTask;
        }

        public async Task Load()
        {
            await _metaNote.Load();

            var it = await _logger.Begin();
            var itEnd = await _logger.End();
            for (; !it.Equals(itEnd); it = await it.Next())
            {
                var logEntry = it.Log;
                var entityRecord = new EntityRecord(0, 0, 0, null);
                entityRecord.DeSerialize(logEntry.Data, logEntry.Size);
                _logEntities.Add(entityRecord.LogIndex, new LogEntity()
                {
                    Term = entityRecord.Term,
                    LogIndex = entityRecord.LogIndex,
                    LogPosition = it.Position,
                    Content = entityRecord.DecreeContent
                });
                if (entityRecord.CommittedLogIndex > _maxCommittedLogIndex)
                {
                    _maxCommittedLogIndex = entityRecord.CommittedLogIndex;
                }

                if (entityRecord.LogIndex > _maxLogIndex)
                {
                    _maxLogIndex = entityRecord.LogIndex;
                }

                if (entityRecord.Term > _maxTerm)
                {
                    _maxTerm = entityRecord.Term;
                }
            }
            if (_maxLogIndex > 0)
            {
                _maxCommittedLogTerm = _logEntities[(UInt64)_maxLogIndex].Term;
            }
        }

        public void Reset()
        {
        }

        public async Task<AppendPosition> AppendEntity(UInt64 term, UInt64 logIndex, UInt64 committedLogIndex, byte[] content)
        {
            await _lock.WaitAsync();
            using (var autoLock = new AutoLock(_lock))
            {
                var entity = GetLogEntity(logIndex);
                if (entity != null && entity.Term == term)
                {
                    return entity.LogPosition;
                }

                var entityRecord = new EntityRecord(term, logIndex, committedLogIndex, content);
                var logEntry = new LogEntry();
                logEntry.Data = entityRecord.Serialize();
                logEntry.Size = logEntry.Data.Length;
                var position = await _logger.AppendLog(logEntry);

                if (entity != null)
                {
                    // if entity exist, remove all the following
                    var lastLogIndex = _logEntities.Last().Key;
                    for (UInt64 index = entity.LogIndex; index <= lastLogIndex; index++)
                    {
                        _logEntities.Remove(index);
                    }
                }
                _logEntities.Add(logIndex, new LogEntity()
                {
                    Term = term,
                    LogIndex = logIndex,
                    LogPosition = position
                });

                if (committedLogIndex > _maxCommittedLogIndex)
                {
                    _maxCommittedLogIndex = committedLogIndex;
                }
                if (logIndex > _maxLogIndex)
                {
                    _maxLogIndex = logIndex;
                }

                if (term > _maxTerm)
                {
                    _maxTerm = term;
                }

                return position;

            }
        }


        public async Task<bool> IsEntityExist(UInt64 logTerm, UInt64 logIndex)
        {
            await _lock.WaitAsync();
            using (var autoLock = new AutoLock(_lock))
            {
                if (logIndex == UInt64.MaxValue)
                {
                    return true;
                }
                if (!_logEntities.ContainsKey(logIndex))
                {
                    return false;
                }

                var entity = _logEntities[logIndex];
                if (entity.Term == logTerm)
                {
                    return true;
                }
                return false;
            }
        }
        public async Task<LogEntity> GetEntityAsync(UInt64 logIndex)
        {
            LogEntity logEntity = null;
            await _lock.WaitAsync();
            using (var autoLock = new AutoLock(_lock))
            {
                logEntity = GetLogEntity(logIndex);
            }

            return logEntity;
        }


        private LogEntity GetLogEntity(UInt64 logIndex)
        {
            if (!_logEntities.ContainsKey(logIndex))
            {
                return null;
            }

            var entity = _logEntities[logIndex];
            return entity;
        }

        public async Task Checkpoint(string newCheckpointFile, ulong logIndex)
        {
            var minOff = await GetMinPositionForLog(logIndex + 1);

           ulong baseLogIndexNo = 0;
            if (RoleMetaRecord != null)
            {
                baseLogIndexNo = RoleMetaRecord.LogIndexNo;
            }

            // write new checkpoint recrod
            var metaRecord = new MetaRecord((UInt64)logIndex, newCheckpointFile, minOff);
            await _metaNote.UpdateMeta(metaRecord);

            // release checkpointed decrees
            ulong newBaseLogIndexNo = 0;
            if (RoleMetaRecord != null)
            {
                newBaseLogIndexNo = RoleMetaRecord.LogIndexNo;
            }
            if (newBaseLogIndexNo > baseLogIndexNo)
            {
                for (var itLogIndexNo = baseLogIndexNo; itLogIndexNo < newBaseLogIndexNo; itLogIndexNo++)
                {
                    //lock(_lock)
                    try
                    {
                        await _lock.WaitAsync();
                        _logEntities.Remove(itLogIndexNo);
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }
            }

            // try to truncate the logs
            await _logger.Truncate(minOff);
        }

        public void Truncate(UInt64 maxDecreeNo, AppendPosition position)
        {
            // remove all the information before maxDecreeNo

            _logger.Truncate(position);
        }

        public SortedDictionary<UInt64, LogEntity> Test_GetAppendLogEntities()
        {
            return _logEntities;
        }

        public MetaRecord RoleMetaRecord => _metaNote?.MetaDataRecord;

        public UInt64 CommittedLogIndex => _maxCommittedLogIndex;
        public UInt64 LastCommittedLogTerm => _maxCommittedLogTerm;

        public UInt64 MaxLogIndex => _maxLogIndex;

        public UInt64 MaxTerm => _maxTerm;


        private Task<AppendPosition> GetMinPositionForLog(ulong logIndex)
        {
            AppendPosition minPos = null;
            Tuple<ulong, AppendPosition> lastSmallerLog = null;
            lock (_checkpointPositionList)
            {
                foreach(var item in _checkpointPositionList)
                {
                    if (item.Item1 > logIndex)
                    {
                        break;
                    }
                    lastSmallerLog = item;
                }
            }

            if (lastSmallerLog != null)
            {
                minPos = lastSmallerLog.Item2;
            }
            if (minPos == null)
            {
                minPos = new AppendPosition(0, 0);
            }

            return Task.FromResult(minPos);
        }

    }

    public class MetaRecord
    {
        private byte[] _magic = new byte[] { (byte)'M', (byte)'e', (byte)'t', (byte)'a' };
        private UInt64 _logIndexNo = 0;
        private string _checkpointFile;
        private AppendPosition _checkpointPosition;

        public MetaRecord(UInt64 logIndexNo, string checkpointFile, AppendPosition checkpointPosition)
        {
            _logIndexNo = logIndexNo;
            _checkpointFile = checkpointFile;
            _checkpointPosition = checkpointPosition;
        }

        public UInt64 LogIndexNo => _logIndexNo;

        public string CheckpointFilePath => _checkpointFile;

        public AppendPosition CheckpointPosition => _checkpointPosition;

        public byte[] Serialize()
        {
            var filePathData = Encoding.UTF8.GetBytes(_checkpointFile);
            var fragmentIndexData = BitConverter.GetBytes((UInt64)_checkpointPosition.FragmentIndex);
            var offsetInFragmentData = BitConverter.GetBytes((UInt64)_checkpointPosition.OffsetInFragment);
            var decreeNoData = BitConverter.GetBytes(_logIndexNo);
            int recrodSize = _magic.Length + filePathData.Length + decreeNoData.Length + fragmentIndexData.Length + offsetInFragmentData.Length;
            var buf = new LogBuffer();
            buf.AllocateBuffer(recrodSize + sizeof(int));
            buf.EnQueueData(BitConverter.GetBytes(recrodSize));
            buf.EnQueueData(_magic);
            buf.EnQueueData(decreeNoData);
            buf.EnQueueData(fragmentIndexData);
            buf.EnQueueData(offsetInFragmentData);
            buf.EnQueueData(filePathData);
            return buf.DataBuf;
        }

        public void DeSerialize(byte[] buf, int len)
        {
            int offInBuf = 0;
            var recordSize = BitConverter.ToInt32(buf, offInBuf);
            offInBuf += sizeof(int);
            var magic = new byte[_magic.Length];
            Array.Copy(buf, offInBuf, magic, 0, magic.Length);
            _magic.SequenceEqual(magic);
            offInBuf += _magic.Length;
            _logIndexNo = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            var fragmentIndex = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            var offsetInFragment = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _checkpointFile = Encoding.UTF8.GetString(buf, offInBuf, len - offInBuf);
            _checkpointPosition = new AppendPosition(fragmentIndex, offsetInFragment);
        }
    }

    public class MetaNote
    {
        private MetaRecord _metaRecord;
        private ILogger _metaLogger;

        public MetaNote(ILogger metaLogger)
        {
            _metaLogger = metaLogger;
        }

        public async Task Load()
        {
            MetaRecord metaRecord = null;
            var it = await _metaLogger.Begin();
            for (; !it.Equals(await _metaLogger.End()); it = await it.Next())
            {
                var entry = it.Log;
                var tmpRecord = new MetaRecord(0, null, null);
                tmpRecord.DeSerialize(entry.Data, entry.Size);
                metaRecord = tmpRecord;
            }
            _metaRecord = metaRecord;
        }

        public async Task UpdateMeta(MetaRecord metaRecord)
        {
            _metaRecord = metaRecord;
            var logEntry = new LogEntry();
            logEntry.Data = _metaRecord.Serialize();
            logEntry.Size = logEntry.Data.Length;
            var poisition = await _metaLogger.AppendLog(logEntry);
            if (poisition.OffsetInFragment > LogSizeThreshold.MetaLogTruncateThreshold)
            {
                // truncate it
                await _metaLogger.Truncate(poisition);
            }
        }

        public MetaRecord MetaDataRecord => _metaRecord;
    }
}
