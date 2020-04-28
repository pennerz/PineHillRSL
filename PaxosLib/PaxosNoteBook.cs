using Paxos.Common;
using Paxos.Message;
using Paxos.Persistence;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Notebook
{
    public class VoteInfo
    {
        public ulong VotedBallotNo { get; set; }
        public PaxosDecree VotedDecree { get; set; }
    }

    public class DecreeBallotInfo: ISer
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        public DecreeBallotInfo()
        {
            DecreeNo = 0;
            NextBallotNo = 0;
            Vote = null;
        }

        public Task AcquireLock()
        {
            return _lock.WaitAsync();
        }
        public void ReleaseLock()
        {
            _lock.Release();
        }
        public UInt64 DecreeNo { get; set; }
        public UInt64 NextBallotNo { get; set; }
        public VoteInfo Vote{ get; set; }

        public byte[] Serialize()
        {
            var decreeNoData = BitConverter.GetBytes(DecreeNo);
            var nextBallotData = BitConverter.GetBytes(NextBallotNo);
            var dataList = new List<byte[]>();
            dataList.Add(decreeNoData);
            dataList.Add(nextBallotData);
            if (Vote != null)
            {
                var votedBalltNoData = BitConverter.GetBytes(Vote.VotedBallotNo);
                var votedDecreeData = Vote.VotedDecree.Data;
                dataList.Add(votedBalltNoData);
                dataList.Add(votedDecreeData);
            }
            var serializeBuf = new SerializeBuffer();
            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;

            /*
            if (Vote != null)
            {
                return "DecreeNo:" + DecreeNo.ToString() + "_" +
                    "NextBallotNo:" + NextBallotNo.ToString() + "_" +
                    "VotedBallotNo:" + Vote.VotedBallotNo.ToString() + "_" +
                    "VotedDecree:" + Vote.VotedDecree.Content + "_";
            }
            else
            {
                return "DecreeNo:" + DecreeNo.ToString() + "_" +
                    "NextBallotNo:" + NextBallotNo.ToString() + "_" ;
            }*/
        }

        public void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            DecreeNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            NextBallotNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            Vote = new VoteInfo();
            Vote.VotedBallotNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            Vote.VotedDecree = new PaxosDecree();
            Vote.VotedDecree.Data = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, Vote.VotedDecree.Data, 0, it.RecordSize);
            /*
            while (str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("DecreeNo"))
                {
                    ulong result = 0;
                    ulong.TryParse(value, out result);
                    DecreeNo = result;
                }
                else if (name.Equals("NextBallotNo"))
                {
                    ulong result = 0;
                    ulong.TryParse(value, out result);
                    NextBallotNo = result;
                }
                else if (name.Equals("VotedBallotNo"))
                {
                    ulong result = 0;
                    ulong.TryParse(value, out result);
                    if (Vote == null)
                    {
                        Vote = new VoteInfo();
                    }
                    Vote.VotedBallotNo = result;
                }
                else if (name.Equals("VotedDecree"))
                {
                    if (Vote == null)
                    {
                        Vote = new VoteInfo();
                    }
                    if (Vote.VotedDecree == null)
                    {
                        Vote.VotedDecree = new PaxosDecree();
                    }
                    Vote.VotedDecree.Content = value;
                }

            }*/
        }
    }

    public class VotedBallotRecord
    {
        private byte[] _magic = new byte[] { (byte)'V', (byte)'o', (byte)'t', (byte)'b' };
        private UInt64 _decreeNo;
        private DecreeBallotInfo _votedBallotInfo;

        public VotedBallotRecord(UInt64 decreeNo, DecreeBallotInfo ballotInfo)
        {
            _decreeNo = decreeNo;
            _votedBallotInfo = ballotInfo;
        }

        public UInt64 DecreeNo => _decreeNo;

        public DecreeBallotInfo VotedBallotInfo => _votedBallotInfo;

        public byte[] Serialize()
        {
            var votedBallotData = _votedBallotInfo.Serialize();
            var decreeNoData = BitConverter.GetBytes(_decreeNo);
            int recrodSize = _magic.Length + votedBallotData.Length + decreeNoData.Length;
            var buf = new Paxos.Persistence.LogBuffer();
            buf.AllocateBuffer(recrodSize + sizeof(int));
            buf.EnQueueData(BitConverter.GetBytes(recrodSize));
            buf.EnQueueData(_magic);
            buf.EnQueueData(decreeNoData);
            buf.EnQueueData(votedBallotData);
            return buf.DataBuf;
        }

        public void DeSerialize(byte[] buf, int len)
        {
            int offInBuf = 0;
            var recordSize = BitConverter.ToInt32(buf, 0);
            offInBuf += sizeof(int);
            var magic = new byte[_magic.Length];
            Array.Copy(buf, offInBuf, magic, 0, magic.Length);
            _magic.SequenceEqual(magic);
            offInBuf += _magic.Length;
            _decreeNo = BitConverter.ToUInt64(buf, sizeof(int));
            offInBuf += sizeof(UInt64);
            //var ballotInfoSerializedStr = Encoding.UTF8.GetString(buf, sizeof(UInt64) + sizeof(int), len - sizeof(UInt64) - sizeof(int));
            var decreeBallotInfoBuf = new byte[len - offInBuf];
            Buffer.BlockCopy(buf, offInBuf, decreeBallotInfoBuf, 0, len - offInBuf);
            var ballotInfo = new DecreeBallotInfo();
            //ballotInfo.DeSerialize(ballotInfoSerializedStr);
            ballotInfo.DeSerialize(decreeBallotInfoBuf);
        }


    }
    public class VoterNote : IDisposable
    {
        // persistent module
        private readonly ILogger _logger;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        // voter role
        private readonly ConcurrentDictionary<ulong, DecreeBallotInfo> _ballotInfo = new ConcurrentDictionary<ulong, DecreeBallotInfo>();

        private List<Tuple<UInt64, AppendPosition>> _checkpointPositionList = new List<Tuple<UInt64, AppendPosition>>();

        private UInt64 _maxDecreeNo = 0;
        private AppendPosition _lastAppendPosition;

        private bool _isStop = false;
        private Task _positionCheckpointTask;

        public VoterNote(ILogger logger)
        {
            _logger = logger;

            _positionCheckpointTask = Task.Run(async () =>
            {
                do
                {
                    await Task.Delay(100000);

                    UInt64 maxDecreeNo = 0;
                    AppendPosition lastAppendPosition;
                    lock (_logger)
                    {
                        maxDecreeNo = _maxDecreeNo;
                        lastAppendPosition = _lastAppendPosition;
                    }

                    if (lastAppendPosition != null)
                    {
                        lock (_checkpointPositionList)
                        {
                            _checkpointPositionList.Add(new Tuple<UInt64, AppendPosition>(maxDecreeNo, lastAppendPosition));
                        }
                    }

                } while (!_isStop);

            });
        }

        public virtual void Dispose()
        { }

        public async Task Load()
        {
            var it = await _logger.Begin();
            var itEnd = await _logger.End();
            for (; !it.Equals(itEnd); it = await it.Next())
            {
                var logEntry = it.Log;
                var ballotRecord = new VotedBallotRecord(0, null);
                try
                {
                    ballotRecord.DeSerialize(logEntry.Data, logEntry.Size);
                }
                catch(Exception)
                {
                    return;
                }
                Logger.Log("Load VotedBallotRecord, decreeNo:{0}", ballotRecord.DecreeNo);

                _ballotInfo.AddOrUpdate(ballotRecord.DecreeNo, ballotRecord.VotedBallotInfo,
                    (key, oldValue) => ballotRecord.VotedBallotInfo);
            }
        }

        public ulong GetNextBallotNo(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = null;
            if (_ballotInfo.TryGetValue(decreeNo, out decreeBallotInfo))
            {
                return decreeBallotInfo.NextBallotNo;
            }
            return 0;
        }

        public void RemoveBallotInfo(ulong decreeNo)
        {
            DecreeBallotInfo ballotInfo;
            while(!_ballotInfo.TryRemove(decreeNo, out ballotInfo));
        }

        public async Task<Tuple<ulong, VoteInfo>> UpdateNextBallotNo(ulong decreeNo, ulong nextBallotNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            ulong oldNextBallotNo = 0;
            await decreeBallotInfo.AcquireLock();
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            var result = new Tuple<ulong, VoteInfo>(oldNextBallotNo, decreeBallotInfo.Vote);
            if (nextBallotNo <= oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return result;
            }
            decreeBallotInfo.NextBallotNo = nextBallotNo;
            var decreeBallotRecord = new VotedBallotRecord(decreeNo, decreeBallotInfo);
            var logEntry = new LogEntry();
            logEntry.Data = decreeBallotRecord.Serialize();
            logEntry.Size = logEntry.Data.Length;
            var appendPosition = await _logger.AppendLog(logEntry);
            lock(_logger)
            {
                if (decreeNo > _maxDecreeNo)
                {
                    _maxDecreeNo = decreeNo;
                }

                if (_lastAppendPosition == null || appendPosition > _lastAppendPosition)
                {
                    _lastAppendPosition = appendPosition;
                }
            }

            decreeBallotInfo.ReleaseLock();

            // for test
            //RemoveBallotInfo(decreeNo);
            return result;
        }

        public VoteInfo GetLastVote(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            return decreeBallotInfo.Vote;
        }

        public async Task<ulong> UpdateLastVote(ulong decreeNo, ulong nextBallotNo, PaxosDecree voteDecree)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            await decreeBallotInfo.AcquireLock();
            ulong oldNextBallotNo = 0;
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            if (nextBallotNo != oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return oldNextBallotNo;
            }

            decreeBallotInfo.Vote = new VoteInfo
            {
                VotedBallotNo = nextBallotNo,
                VotedDecree = voteDecree
            };

            var votedBallotRecord = new VotedBallotRecord(decreeNo, decreeBallotInfo);
            var logEntry = new LogEntry();
            logEntry.Data = votedBallotRecord.Serialize();
            logEntry.Size = logEntry.Data.Length;

            var appendPosition = await _logger.AppendLog(logEntry);
            lock (_logger)
            {
                if (_lastAppendPosition == null || appendPosition > _lastAppendPosition)
                {
                    _lastAppendPosition = appendPosition;
                }
            }

            decreeBallotInfo.ReleaseLock();

            // test
            //RemoveBallotInfo(decreeNo);

            return oldNextBallotNo;
        }

        public void Reset()
        {
            _ballotInfo.Clear();
        }

        private DecreeBallotInfo GetDecreeBallotInfo(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = null;
            if (_ballotInfo.TryGetValue(decreeNo, out decreeBallotInfo))
            {
                return decreeBallotInfo;
            }

            return null;
        }

        private DecreeBallotInfo AddDecreeBallotInfo(ulong decreeNo)
        {
            do
            {
                _ballotInfo.TryAdd(decreeNo, new DecreeBallotInfo()
                {
                     DecreeNo = decreeNo
                });
                var decreeBallotInfo = GetDecreeBallotInfo(decreeNo);
                if (decreeBallotInfo != null)
                {
                    return decreeBallotInfo;
                }
            } while (true);
        }
        public AppendPosition GetMaxPositionForDecrees(ulong decreeNo)
        {
            AppendPosition minPos = null;
            lock (_checkpointPositionList)
            {
                foreach (var item in _checkpointPositionList)
                {
                    if (item.Item1 > decreeNo)
                    {
                        break;
                    }
                    minPos = item.Item2;
                }
            }

            if (minPos == null)
            {
                minPos = new AppendPosition(0, 0, 0);
            }

            return minPos;
        }

        public void Truncate(UInt64 maxDecreeNo, AppendPosition position)
        {
            // remove all the information before maxDecreeNo

            _logger.Truncate(position);
        }

    }

    public class MetaRecord
    {
        private byte[] _magic = new byte[] { (byte)'M', (byte)'e', (byte)'t', (byte)'a' };
        private UInt64 _decreeNo = 0;
        private string _checkpointFile;
        private AppendPosition _checkpointPosition;

        public MetaRecord(UInt64 decreeNo, string checkpointFile, AppendPosition checkpointPosition)
        {
            _decreeNo = decreeNo;
            _checkpointFile = checkpointFile;
            _checkpointPosition = checkpointPosition;
        }

        public UInt64 DecreeNo => _decreeNo;

        public string CheckpointFilePath => _checkpointFile;

        public AppendPosition CheckpointPosition => _checkpointPosition;

        public byte[] Serialize()
        {
            var filePathData = Encoding.UTF8.GetBytes(_checkpointFile);
            var fragmentIndexData = BitConverter.GetBytes((UInt64)_checkpointPosition.FragmentIndex);
            var offsetInFragmentData = BitConverter.GetBytes((UInt64)_checkpointPosition.OffsetInFragment);
            var decreeNoData = BitConverter.GetBytes(_decreeNo);
            int recrodSize = _magic.Length + filePathData.Length + decreeNoData.Length + fragmentIndexData.Length + offsetInFragmentData.Length;
            var buf = new Paxos.Persistence.LogBuffer();
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
            _decreeNo = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            var fragmentIndex = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            var offsetInFragment = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _checkpointFile = Encoding.UTF8.GetString(buf, offInBuf, len - offInBuf);
            _checkpointPosition = new AppendPosition(fragmentIndex, offsetInFragment, 0);
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


    public class CommittedDecreeRecord
    {
        private byte[] _magic = new byte[] { (byte)'C', (byte)'o', (byte)'m', (byte)'t' };
        private UInt64 _decreeNo = 0;
        private byte[] _decreeContent;

        public CommittedDecreeRecord(UInt64 decreeNo, byte[] decreeContent)
        {
            _decreeNo = decreeNo;
            _decreeContent = decreeContent;
        }
        public UInt64 DecreeNo => _decreeNo;

        public byte[] DecreeContent => _decreeContent;

        public byte[] Serialize()
        {
            var decreeContentData = _decreeContent; // Encoding.UTF8.GetBytes(_decreeContent);
            var decreeNoData = BitConverter.GetBytes(_decreeNo);
            int recrodSize = _magic.Length + decreeContentData.Length + decreeNoData.Length;
            var buf = new Paxos.Persistence.LogBuffer();
            buf.AllocateBuffer(recrodSize + sizeof(int));
            buf.EnQueueData(BitConverter.GetBytes(recrodSize));
            buf.EnQueueData(_magic);
            buf.EnQueueData(decreeNoData);
            buf.EnQueueData(decreeContentData);
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
            _decreeNo = BitConverter.ToUInt64(buf, offInBuf);
            offInBuf += sizeof(UInt64);
            _decreeContent = new byte[len - offInBuf];
            Buffer.BlockCopy(buf, offInBuf, _decreeContent, 0, _decreeContent.Length);
            //_decreeContent = Encoding.UTF8.GetString(buf, sizeof(UInt64) + sizeof(int), len - sizeof(UInt64) - sizeof(int));
        }

    }
    public class ProposerNote : IDisposable
    {
        public class CommittedDecreeInfo
        {
            public UInt64 CheckpointedDecreeNo { get; set; }
            public PaxosDecree CommittedDecree { get; set; }
        }
        private class RequestPositionItemComparer : SlidingWindow.IItemComparer
        {
            public bool IsSmaller(object left, object right)
            {
                if (left == null) return true;
                if (right == null) return false;
                var leftpos = left as AppendPosition;
                var rightpos = right as AppendPosition;
                if (leftpos == null) return true;
                if (rightpos == null) return false;

                return leftpos < rightpos;
            }
        }
        // proposer role
        //private ConcurrentDictionary<ulong, Propose> _decreeState = new ConcurrentDictionary<ulong, Propose>();
        //private Ledger _ledger;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly ConcurrentDictionary<ulong, PaxosDecree> _committedDecrees = new ConcurrentDictionary<ulong, PaxosDecree>();
        private ILogger _logger;
        private ILogger _metaLogger;
        private SlidingWindow _activePropose = new SlidingWindow(0, new RequestPositionItemComparer());
        private RequestPositionItemComparer _positionComparer = new RequestPositionItemComparer();
        private Task _positionCheckpointTask;
        private List<Tuple<ulong, AppendPosition>> _checkpointPositionList = new List<Tuple<ulong, AppendPosition>>();
        private bool _isStop = false;
        private MetaNote _metaNote = null;

        public ProposerNote(ILogger logger, ILogger metaLogger = null)
        {
            if (logger == null)
            {
                throw new ArgumentNullException("IPaxosCommitedDecreeLogger");
            }
            _metaLogger = metaLogger;
            _logger = logger;
            if (_metaLogger != null)
            {
                _metaNote = new MetaNote(_metaLogger);
            }
            _positionCheckpointTask = Task.Run(async () =>
            {
                do
                {
                    await Task.Delay(1000);

                    UInt64 minDecreeNo = 0;
                    object minValue = null;
                    _activePropose.GetSmallestItem(out minDecreeNo, out minValue);
                    var position = minValue as AppendPosition;
                    if (position != null)
                    {
                        lock (_checkpointPositionList)
                        {
                            _checkpointPositionList.Add(new Tuple<ulong, AppendPosition>(minDecreeNo, position));
                        }
                    }

                } while (!_isStop);

            });
        }

        public virtual void Dispose()
        {
            _isStop = true;
        }

        public async Task Load()
        {
            // load from meta
            if (_metaNote != null)
            {
                await _metaNote.Load();
            }
            var baseDecreeNo = ProposeRoleMetaRecord != null ? (UInt64)ProposeRoleMetaRecord.DecreeNo : (UInt64)0;
            _activePropose = new SlidingWindow(baseDecreeNo, new RequestPositionItemComparer());

            if (ProposeRoleMetaRecord != null)
            {
                _logger.SetBeginPosition(ProposeRoleMetaRecord.CheckpointPosition);
            }
            var it = await _logger.Begin();
            var itEnd = await _logger.End();
            for (; !it.Equals(itEnd); it = await it.Next())
            {
                var entry = it.Log;
                var tmpRecord = new CommittedDecreeRecord(0, null);
                tmpRecord.DeSerialize(entry.Data, entry.Size);
                Logger.Log("Load committed decree record: DecreeNo:{0}", tmpRecord.DecreeNo);
                if (tmpRecord.DecreeContent == null)
                {
                    Logger.Log("Load committed decree record: with empty decree, skip it");
                    continue;
                }

                if (tmpRecord.DecreeNo < baseDecreeNo)
                {
                    Logger.Log("Load committed decree record is smaller than baseDecreeNo{0}, skip it", baseDecreeNo);
                    continue;
                }
                var decree = new PaxosDecree(Encoding.UTF8.GetString(tmpRecord.DecreeContent));
                _committedDecrees.AddOrUpdate(tmpRecord.DecreeNo, decree,
                    (key, oldValue) => decree);
            }
        }

        /// <summary>
        /// Get the maximum committed decree no
        /// </summary>
        /// <returns></returns>
        public ulong GetMaximumCommittedDecreeNo()
        {
            ulong maximumCommittedDecreeNo = 0;
            lock(_lock)
            {
                maximumCommittedDecreeNo = GetMaximumCommittedDecreeNoNeedLock();
            }
            return maximumCommittedDecreeNo;
        }
        

        /// <summary>
        /// Get the committed decree
        /// </summary>
        /// <param name="decreeNo"></param>
        /// <returns></returns>
        public Task<CommittedDecreeInfo> GetCommittedDecree(ulong decreeNo)
        {
            // committed dcree can never be changed
            PaxosDecree committedDecree = null;
            var committedDecreeInfo = new CommittedDecreeInfo();
            lock (_lock)
            {
                if (ProposeRoleMetaRecord != null && ProposeRoleMetaRecord.DecreeNo > decreeNo)
                {
                    committedDecreeInfo.CheckpointedDecreeNo = ProposeRoleMetaRecord.DecreeNo;
                }

                if (decreeNo > committedDecreeInfo.CheckpointedDecreeNo)
                {
                    if (_committedDecrees.TryGetValue(decreeNo, out committedDecree))
                    {
                        committedDecreeInfo.CommittedDecree = committedDecree;
                    }
                }
            }
            return Task.FromResult(committedDecreeInfo);
        }

        public Task<AppendPosition> CommitDecree(ulong decreeNo, PaxosDecree commitedDecree)
        {
            return CommitDecreeInternal(decreeNo, commitedDecree);
        }

        public async Task<List<KeyValuePair<ulong, PaxosDecree>>> GetFollowingCommittedDecress(ulong beginDecreeNo)
        {
            var committedDecrees = new List<KeyValuePair<ulong, PaxosDecree>>();
            lock (_lock)
            {
                for (ulong decreeNo = beginDecreeNo; decreeNo <= GetMaximumCommittedDecreeNo(); decreeNo++)
                {
                    PaxosDecree committedDecree = null;
                    if (_committedDecrees.TryGetValue(decreeNo, out committedDecree))
                    {
                        committedDecrees.Add(new KeyValuePair<ulong, PaxosDecree>(decreeNo, committedDecree));
                    }
                }
            }
            return committedDecrees.Count > 0 ? committedDecrees : null;
        }
        public ulong GetCommittedDecreeCount()
        {
            lock(_lock)
            {
                return (ulong)_committedDecrees.Count;
            }
        }

        public void Clear()
        {
            lock(_lock)
            {
                _committedDecrees.Clear();
            }
        }

        public async Task Checkpoint(string newCheckpointFile, ulong decreeNo)
        {
            var minOff = await GetMinPositionForDecrees(decreeNo + 1);

            ulong baseDecreeNo = 0;
            if (ProposeRoleMetaRecord != null)
            {
                baseDecreeNo = ProposeRoleMetaRecord.DecreeNo;
            }
            // write new checkpoint recrod
            var metaRecord = new MetaRecord((UInt64)decreeNo, newCheckpointFile, minOff);
            await _metaNote.UpdateMeta(metaRecord);

            // release checkpointed decrees
            ulong newBaseDecreNo = 0;
            if (ProposeRoleMetaRecord != null)
            {
                newBaseDecreNo = ProposeRoleMetaRecord.DecreeNo;
            }
            if (newBaseDecreNo > baseDecreeNo)
            {
                for(var itDecreeNo = baseDecreeNo; decreeNo < newBaseDecreNo; itDecreeNo++)
                {
                    lock(_lock)
                    {
                        PaxosDecree paxosDecree = null;
                        _committedDecrees.TryRemove(itDecreeNo, out paxosDecree);
                    }
                }
            }

            // try to truncate the logs
            await _logger.Truncate(minOff);
        }

        private async Task<AppendPosition> GetMinPositionForDecrees(ulong beginDecreeNo)
        {
            AppendPosition minPos = null;
            Tuple<ulong, AppendPosition> lastSmallerDecree = null;
            lock (_checkpointPositionList)
            {
                foreach(var item in _checkpointPositionList)
                {
                    if (item.Item1 > beginDecreeNo)
                    {
                        break;
                    }
                    lastSmallerDecree = item;
                }
            }

            if (lastSmallerDecree != null)
            {
                minPos = lastSmallerDecree.Item2;
            }
            if (minPos == null)
            {
                minPos = new AppendPosition(0, 0, 0);
            }

            return minPos;
        }

        private ulong GetMaximumCommittedDecreeNoNeedLock()
        {
            lock(_lock)
            {
                var maxDecreeNo = _committedDecrees.LastOrDefault().Key;
                if (ProposeRoleMetaRecord != null && ProposeRoleMetaRecord.DecreeNo > maxDecreeNo)
                {
                    maxDecreeNo = ProposeRoleMetaRecord.DecreeNo;
                }
                return maxDecreeNo;
            }
        }
        

        private async Task<AppendPosition> CommitDecreeInternal(ulong decreeNo, PaxosDecree decree)
        {
            // Paxos protocol already make sure the consistency.
            // So the decree will be idempotent, just write it directly
            var commitDecreeRecord = new CommittedDecreeRecord(decreeNo, Encoding.UTF8.GetBytes(decree.Content));
            var logEntry = new LogEntry();
            logEntry.Data = commitDecreeRecord.Serialize();
            logEntry.Size = logEntry.Data.Length;

            var position = await _logger.AppendLog(logEntry);
            //return position;

            // add it in cache
            lock (_lock)
            //try
            {
                //await _lock.WaitAsync();
                do
                {
                    if (_committedDecrees.ContainsKey(decreeNo))
                    {
                        break;
                    }
                    if (_committedDecrees.TryAdd(decreeNo, decree))
                    {
                        break;
                    }
                    //throw new Exception("decree already committed");
                } while (true);
            }
            //finally
            {
                //_lock.Release();
            }

            _activePropose.Add(decreeNo, position);
            while ((_activePropose.Pop()) != null) ;

            return position;
        }

        public MetaRecord ProposeRoleMetaRecord => _metaNote?.MetaDataRecord;

        public IReadOnlyDictionary<ulong, PaxosDecree> CommittedDecrees => _committedDecrees;
    }

}
