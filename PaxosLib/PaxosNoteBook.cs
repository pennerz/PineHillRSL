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
        public ulong DecreeNo { get; set; }
        public ulong NextBallotNo { get; set; }
        public VoteInfo Vote{ get; set; }

        public string Serialize()
        {
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
            }
        }

        public void DeSerialize(string str)
        {
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

            }
        }
    }

    public class VoterNote : IDisposable
    {
        // persistent module
        private readonly IPaxosVotedBallotLog _logger;
        // voter role
        private readonly ConcurrentDictionary<ulong, DecreeBallotInfo> _ballotInfo = new ConcurrentDictionary<ulong, DecreeBallotInfo>();

        private List<Tuple<UInt64, AppendPosition>> _checkpointPositionList = new List<Tuple<UInt64, AppendPosition>>();

        private UInt64 _maxDecreeNo = 0;
        private AppendPosition _lastAppendPosition;

        private bool _isStop = false;
        private Task _positionCheckpointTask;

        public VoterNote(IPaxosVotedBallotLog logger)
        {
            _logger = logger;

            _positionCheckpointTask = Task.Run(async () =>
            {
                do
                {
                    await Task.Delay(1000);

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
                _ballotInfo.AddOrUpdate(it.VotedBallot.Item1, it.VotedBallot.Item2,
                    (key, oldValue) => it.VotedBallot.Item2);
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
            var appendPosition = await _logger.AppendLog(decreeNo, decreeBallotInfo);
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

            var appendPosition = await _logger.AppendLog(decreeNo, decreeBallotInfo);
            lock (_logger)
            {
                if (_lastAppendPosition == null || appendPosition > _lastAppendPosition)
                {
                    _lastAppendPosition = appendPosition;
                }
            }

            decreeBallotInfo.ReleaseLock();
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
        private UInt64 _decreeNo = 0;
        private string _checkpointFile;

        public MetaRecord(UInt64 decreeNo, string checkpointFile)
        {
            _decreeNo = decreeNo;
            _checkpointFile = checkpointFile;
        }

        public UInt64 DecreeNo => _decreeNo;

        public string CheckpointFilePath => _checkpointFile;

        public byte[] Serialize()
        {
            var filePathData = Encoding.UTF8.GetBytes(_checkpointFile);
            var decreeNoData = BitConverter.GetBytes(_decreeNo);
            int recrodSize = filePathData.Length + decreeNoData.Length;
            var sizeData = BitConverter.GetBytes(recrodSize);
            var buf = new Paxos.Persistence.Buffer();
            buf.AllocateBuffer(recrodSize + sizeData.Length);
            buf.EnQueueData(sizeData);
            buf.EnQueueData(decreeNoData);
            buf.EnQueueData(filePathData);
            return buf.DataBuf;
        }

        public void DeSerialize(byte[] buf)
        {
            var recordSize = BitConverter.ToInt32(buf, 0);
            if (recordSize < sizeof(int) + sizeof(UInt64))
            {
                return;
            }
            _decreeNo = BitConverter.ToUInt64(buf, sizeof(int));
            _checkpointFile = Encoding.UTF8.GetString(buf, sizeof(int) + sizeof(UInt64), recordSize - sizeof(UInt64));
        }
    }

    public class ProposerNote : IDisposable
    {
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
        private readonly ConcurrentDictionary<ulong, PaxosDecree> _committedDecrees = new ConcurrentDictionary<ulong, PaxosDecree>();
        private IPaxosCommitedDecreeLog _logger;
        private SlidingWindow _activePropose = new SlidingWindow(0, new RequestPositionItemComparer());
        private RequestPositionItemComparer _positionComparer = new RequestPositionItemComparer();
        private Task _positionCheckpointTask;
        private List<Tuple<ulong, AppendPosition>> _checkpointPositionList = new List<Tuple<ulong, AppendPosition>>();
        private bool _isStop = false;

        private MetaRecord _metaRecord = null;

        public ProposerNote(IPaxosCommitedDecreeLog logger)
        {
            if (logger == null)
            {
                throw new ArgumentNullException("IPaxosCommitedDecreeLogger");
            }
            _logger = logger;
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
            ulong baseDecreeNo = 0;
            _activePropose = new SlidingWindow(baseDecreeNo, new RequestPositionItemComparer());

            // write new checkpoint recrod
            var metaRecord = new MetaRecord(0, "");

            var recordBuff = new byte[1024 * 1024];
            string fileMetaFilePath = "paxos.meta";
            var metaStream = new FileStream(fileMetaFilePath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            do
            {
                var readlen = await metaStream.ReadAsync(recordBuff, 0, sizeof(int));
                if (readlen != sizeof(int))
                {
                    break;
                }
                var recordSize = BitConverter.ToInt32(recordBuff, 0);
                if (recordSize < 0)
                {
                    break;
                }
                readlen = await metaStream.ReadAsync(recordBuff, sizeof(int), recordSize);
                if (readlen != recordSize)
                {
                    break;
                }
                metaRecord.DeSerialize(recordBuff);
            } while (true);

            _metaRecord = metaRecord;

            var it = await _logger.Begin();
            var itEnd = await _logger.End();
            for (; !it.Equals(itEnd); it = await it.Next())
            {
                if (it.DecreeNo < metaRecord.DecreeNo)
                {
                    continue;
                }
                _committedDecrees.AddOrUpdate(it.DecreeNo, it.Decree,
                    (key, oldValue) => it.Decree);
            }
           _activePropose = new SlidingWindow(metaRecord.DecreeNo, new RequestPositionItemComparer());

    }

    /// <summary>
    /// Get the maximum committed decree no
    /// </summary>
    /// <returns></returns>
    public ulong GetMaximumCommittedDecreeNo()
        {
            ulong maximumCommittedDecreeNo = 0;
            lock(_logger)
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
        public Task<PaxosDecree> GetCommittedDecree(ulong decreeNo)
        {
            // committed dcree can never be changed
            PaxosDecree committedDecree = null;
            if (!_committedDecrees.TryGetValue(decreeNo, out committedDecree))
            {
                committedDecree = null;
            }
            return Task.FromResult(committedDecree);
        }

        public Task<AppendPosition> CommitDecree(ulong decreeNo, PaxosDecree commitedDecree)
        {
            return CommitDecreeInternal(decreeNo, commitedDecree);
        }

        public List<KeyValuePair<ulong, PaxosDecree>> GetFollowingCommittedDecress(ulong beginDecreeNo)
        {
            var committedDecrees = new List<KeyValuePair<ulong, PaxosDecree>>();
            lock (_logger)
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
            return (ulong)_committedDecrees.Count;
        }

        public void Clear()
        {
            _committedDecrees.Clear();
        }

        public async Task Checkpoint(string newCheckpointFile, ulong decreeNo)
        {
            // write new checkpoint recrod
            var metaRecord = new MetaRecord((UInt64)decreeNo, newCheckpointFile);
            string fileMetaFilePath = "paxos.meta";
            var metaStream = new FileStream(fileMetaFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
            var data = metaRecord.Serialize();
            await metaStream.WriteAsync(data, 0, data.Length);

            // try to truncate the logs
            var minOff = GetMinPositionForDecrees(decreeNo + 1);
            await _logger.Truncate(minOff);
        }

        private AppendPosition GetMinPositionForDecrees(ulong beginDecreeNo)
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
            return _committedDecrees.LastOrDefault().Key;
        }
        

        private async Task<AppendPosition> CommitDecreeInternal(ulong decreeNo, PaxosDecree decree)
        {
            // Paxos protocol already make sure the consistency.
            // So the decree will be idempotent, just write it directly

            var position = await _logger.AppendLog(decreeNo, decree);

            // add it in cache
            lock (_committedDecrees)
            {
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

            _activePropose.Add(decreeNo, position);
            while (_activePropose.Pop() != null) ;

            return position;
        }

        public MetaRecord ProposeRoleMetaRecord => _metaRecord;

        public IReadOnlyDictionary<ulong, PaxosDecree> CommittedDecrees => _committedDecrees;
    }

}
