﻿using Paxos.Notebook;
using Paxos.Request;
using Paxos.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Persistence
{
    public enum IteratorType { Default, End};

    public class LogAppendRequest
    {
        public int Offset { get; set; }

        public TaskCompletionSource<AppendPosition> Result = new TaskCompletionSource<AppendPosition>();
    }

    public interface ICommittedDecreeIterator : IEquatable<ICommittedDecreeIterator>, IDisposable
    {
        PaxosDecree Decree { get; }
        ulong DecreeNo { get; }
        Task<ICommittedDecreeIterator> Next();
    }

    public interface IPaxosCommitedDecreeLog : IDisposable
    {
        Task<ICommittedDecreeIterator> Begin();
        Task<ICommittedDecreeIterator> End();
        Task<AppendPosition> AppendLog(ulong decreeNo, PaxosDecree decree);
        Task Truncate(AppendPosition posotion);
    }

    public interface IVotedBallotIterator : IEquatable<IVotedBallotIterator>, IDisposable
    {
        Tuple<ulong, DecreeBallotInfo> VotedBallot { get; }
        Task<IVotedBallotIterator> Next();
    }

    public interface IPaxosVotedBallotLog : IDisposable
    {
        Task<IVotedBallotIterator> Begin();
        Task<IVotedBallotIterator> End();

        Task<AppendPosition> AppendLog(ulong decreeNo, DecreeBallotInfo decreeBallotInfo);
    }

    public class LogEntry
    {
        uint Size { get; set; }
        byte[] Data { get; set; }
    }

    public class FileCommittedDecreeIterator : ICommittedDecreeIterator
    {
        private FileStream _dataStream;
        private PaxosDecree _currentDecree;
        private ulong _currentDecreeNo;
        private IteratorType _itType = IteratorType.Default;

        public FileCommittedDecreeIterator(
            FileStream dataStream,
            ulong decreeNo,
            PaxosDecree decree,
            IteratorType itType)
        {
            _dataStream = dataStream;
            _currentDecreeNo = decreeNo;
            _currentDecree = decree;
            _itType = itType;
        }
        public virtual void Dispose()
        {
           // _dataStream?.Close();
        }

        public PaxosDecree Decree => _currentDecree;

        public ulong DecreeNo => _currentDecreeNo;

        public async Task<ICommittedDecreeIterator> Next()
        {
            FileCommittedDecreeIterator next = null;

            var databuf = new byte[sizeof(uint)];
            var readlen = await _dataStream.ReadAsync(databuf, 0, sizeof(uint));
            if (readlen != sizeof(uint))
            {
                // not enough data
                _dataStream.Close();
                return new FileCommittedDecreeIterator(null, 0, null, IteratorType.End);
            }
            uint dataSize = BitConverter.ToUInt32(databuf, 0);
            databuf = new byte[dataSize];
            readlen = await _dataStream.ReadAsync(databuf, 0, (int)dataSize);
            if (readlen != dataSize)
            {
                // not enough data
                _dataStream.Close();
                return new FileCommittedDecreeIterator(null, 0, null, IteratorType.End);
            }

            var data = Encoding.UTF8.GetString(databuf, (int)sizeof(ulong), (int)(dataSize - sizeof(ulong)));
            var decree = Serializer<PaxosDecree>.Deserialize(data);
            ulong decreeNo = BitConverter.ToUInt64(databuf, 0);

            next = new FileCommittedDecreeIterator(_dataStream, decreeNo, decree, IteratorType.Default);

            return next;
        }

        public bool Equals(ICommittedDecreeIterator rhs)
        {
            var rhsFileIt = rhs as FileCommittedDecreeIterator;
            if (rhsFileIt == null)
            {
                return false;
            }
            if (_itType == rhsFileIt._itType && _itType == IteratorType.End)
            {
                return true;
            }
            if (_itType != rhsFileIt._itType)
            {
                return false;
            }
            if (_dataStream != rhsFileIt._dataStream)
            {
                return false;
            }
            if (_dataStream.Position != rhsFileIt._dataStream.Position)
            {
                return false;
            }
            if (!_currentDecree.Equals(rhsFileIt._currentDecree))
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            var rhs = obj as ICommittedDecreeIterator;
            return Equals(rhs);
        }

        public override int GetHashCode()
        {
            return 1;
        }
    }

    class Buffer
    {
        private byte[] _buffer = null;
        private int _bufLen = 0;
        public byte[] DataBuf => _buffer;
        public int BufLen => _bufLen;
        public int Length { get; set; }

        public Buffer()
        {
            Length = 0;
        }
        public void AllocateBuffer(int size)
        {
            var newBuffer = new byte[size];
            if (_buffer != null & BufLen != 0)
            {
                _buffer.CopyTo(newBuffer, 0);
            }
            _buffer = newBuffer;
            _bufLen = size;
        }

        public void EnQueueData(byte[] data)
        {
            if (data == null || data.Length == 0) return;
            if (Length + data.Length > _bufLen)
            {
                AllocateBuffer(_bufLen * 2);
            }
            data.CopyTo(_buffer, Length);
            Length += data.Length;
        }
    }

    public class TruncateRequest
    {
        public AppendPosition Position { get; set; }
        public TaskCompletionSource<bool> Result { get; set; }
    }

    public class AppendPosition
    {
        public AppendPosition(UInt64 fragementIndex, UInt64 offsetInFragment, UInt64 totalOffset)
        {
            FragmentIndex = fragementIndex;
            OffsetInFragment = offsetInFragment;
            TotalOffset = totalOffset;
        }
        public UInt64 FragmentIndex { get; set; }
        public UInt64 OffsetInFragment { get; set; }
        public UInt64 TotalOffset { get; set; }

        public static bool operator<(AppendPosition left, AppendPosition right)
        {
            if (left == null)
            {
                if (right == null)
                {
                    return false;
                }
                return true;
            }
            if (right == null)
            {
                return false;
            }
            if (left.FragmentIndex < right.FragmentIndex)
            {
                return true;
            }
            if (left.FragmentIndex > right.FragmentIndex)
            {
                return false;
            }
            return left.OffsetInFragment < right.OffsetInFragment;
        }
        public static bool operator >(AppendPosition left, AppendPosition right)
        {
            if (right == null)
            {
                if (left == null)
                {
                    return false;
                }
                return true;
            }
            if (left == null)
            {
                return false;
            }
            if (left.FragmentIndex > right.FragmentIndex)
            {
                return true;
            }
            if (left.FragmentIndex < right.FragmentIndex)
            {
                return false;
            }
            return left.OffsetInFragment > right.OffsetInFragment;
        }
    }

    public class BaseLogWriter : IDisposable
    {
        private string _dataFilePath;
        private FileStream _dataStream;
        private List<UInt64> _prevDataStreamLength = new List<UInt64>();
        private UInt64 _baseFragmentIndex = 0;
        private UInt64 _currentFragmentIndex = 0;
        private UInt64 _fragmentSizeLimit = 1024;
        private UInt64 _fragmentBaseOffset = 0;

        private List<Buffer> _ringBuffer = new List<Buffer>();
        private int _currentIndex;
        private int _off = 0;
        private bool _ongoing = false;
        private List<LogAppendRequest> _appendRequestsQueue = new List<LogAppendRequest>();
        private SemaphoreSlim _pendingRequestLock = new SemaphoreSlim(0);
        private Task _appendTask;
        private List<TruncateRequest> _truncateRequestList = new List<TruncateRequest>();

        public BaseLogWriter(string dataFilePath)
        {
            IsStop = false;
            _dataFilePath = dataFilePath + "_";
            for (int i = 0; i < 2; i++)
            {
                var buffer = new Buffer();
                buffer.AllocateBuffer(1024 * 1024);
                _ringBuffer.Add(buffer);
            }


            _appendTask = Task.Run(async () =>
            {
                do
                {
                    await _pendingRequestLock.WaitAsync();

                    byte[] writeBuf = null;
                    int writeEndOff = 0;
                    int bufLen = 0;

                    var begin = DateTime.Now;

                    TruncateRequest truncateRequest = null;
                    lock (_truncateRequestList)
                    {
                        if (_truncateRequestList.Count > 0)
                        {
                            truncateRequest = _truncateRequestList[0];
                        }
                    }
                    if (truncateRequest != null)
                    {
                        await TruncateInlock(truncateRequest.Position);
                        lock (_truncateRequestList)
                        {
                            _truncateRequestList.RemoveAt(0);
                        }
                        truncateRequest.Result.SetResult(true);

                    }

                    lock (_ringBuffer)
                    {
                        var curBuffer = _ringBuffer[_currentIndex];

                        writeBuf = curBuffer.DataBuf;
                        bufLen = curBuffer.Length;
                        writeEndOff = _off;
                        _currentIndex++;
                        _currentIndex %= _ringBuffer.Count;
                        curBuffer = _ringBuffer[_currentIndex];
                        curBuffer.Length = 0;
                    }
                    var getBufTime = DateTime.Now - begin;
                    if (writeBuf == null || bufLen == 0)
                    {
                        continue;
                    }

                    OpenForAppend();

                    await _dataStream.WriteAsync(writeBuf, 0, bufLen);
                    await _dataStream.FlushAsync();

                    List<LogAppendRequest> finishedReqList = new List<LogAppendRequest>();
                    var writeStreamTime = DateTime.Now - begin - getBufTime;
                    lock (_ringBuffer)
                    {
                        foreach (var req in _appendRequestsQueue)
                        {
                            if (writeEndOff >= req.Offset)
                            {
                                finishedReqList.Add(req);
                            }
                            else
                            {
                                break;
                            }
                        }
                        _appendRequestsQueue.RemoveRange(0, finishedReqList.Count);
                    }
                    var currentFragmentIndex = _currentFragmentIndex;
                    var currentFragmentBaseOff = _fragmentBaseOffset;
                    var task = Task.Run(() =>
                    {
                        var begin = DateTime.Now;
                        foreach (var req in finishedReqList)
                        {
                            req.Result.SetResult(new AppendPosition(currentFragmentIndex, (UInt64)req.Offset - currentFragmentBaseOff, (UInt64)req.Offset));
                        }
                        var notifyTime = DateTime.Now - begin;
                        if (notifyTime.TotalMilliseconds > 500)
                        {
                            //Console.WriteLine("too slow");
                        }
                    });
                    var notifyRequesTime = DateTime.Now - begin - getBufTime - writeStreamTime;

                    var totoalTime = DateTime.Now - begin;
                    if (totoalTime.TotalMilliseconds > 500)
                    {
                        //Console.WriteLine("too slow");
                    }

                } while (!IsStop);
            });
        }

        public async Task<AppendPosition> AppendLog(byte[] datablock1, byte[] datablock2, byte[] datablock3)
        {
            var begin = DateTime.Now;
            var request = new LogAppendRequest();
            var objAllocatedTime = DateTime.Now - begin;

            var blockLength = datablock1.Length;
            if (datablock2 != null) blockLength += datablock2.Length;
            if (datablock3 != null) blockLength += datablock3.Length;

            TimeSpan lockWaitTime;
            var beforeLock = DateTime.Now;
            lock (_ringBuffer)
            {
                var curBuffer = _ringBuffer[_currentIndex];

                lockWaitTime = DateTime.Now - beforeLock;
                DateTime beforeCopy = DateTime.Now;
                if (blockLength + curBuffer.Length > curBuffer.BufLen)
                {
                    // increase the buffer size
                    curBuffer.AllocateBuffer(curBuffer.BufLen * 2);
                }
                curBuffer.EnQueueData(datablock1);
                if (datablock2 != null)
                {
                    curBuffer.EnQueueData(datablock2);
                }
                if (datablock3 != null)
                {
                    curBuffer.EnQueueData(datablock3);
                }
                _off += blockLength;
                request.Offset = _off;
                _appendRequestsQueue.Add(request);
            }
            _pendingRequestLock.Release();

            var requestInQueueTime = DateTime.Now - begin - objAllocatedTime ;


            var taskCreateTime = DateTime.Now - begin - objAllocatedTime  - requestInQueueTime;

            var appendPosition = await request.Result.Task;

            var appendTime = DateTime.Now - begin;
            if (appendTime.TotalMilliseconds > 500)
            {
                //Console.WriteLine("too slow");
            }

            return appendPosition;
        }

        public Task Truncate(AppendPosition position)
        {
            var reqeust = new TruncateRequest()
            {
                Position = position,
                Result = new TaskCompletionSource<bool>()
            };
            lock(_truncateRequestList)
            {
                if (_truncateRequestList.Count == 0)
                {
                    _truncateRequestList.Add(reqeust);  // only one request allowed
                }
            }

            _pendingRequestLock.Release();
            return reqeust.Result.Task;
        }

        public void OpenForAppend()
        {
            if (_dataStream != null &&  _dataStream.Position < (long)_fragmentSizeLimit)
            {
                return;
            }
            if (_dataStream != null)
            {
                _currentFragmentIndex++;
                _fragmentBaseOffset += (UInt64)_dataStream.Position;
                _prevDataStreamLength.Add((UInt64)_dataStream.Position);
            }
            _dataStream?.Close();
            _dataStream?.Dispose();
            _dataStream = new FileStream(_dataFilePath + _currentFragmentIndex.ToString(), FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
        }

        public void Dispose()
        {
            _dataStream?.Close();
            _dataStream?.Dispose();
        }

        public bool IsStop { get; set; }

        private async Task<bool> TruncateInlock(AppendPosition position)
        {
            // remove all the files before the current fragment index
            if (_baseFragmentIndex >= position.FragmentIndex)
            {
                return false; // nothing to truncate
            }
            UInt64 truncateDataSize = 0;
            for (UInt64 i = _baseFragmentIndex; i < position.FragmentIndex; i++)
            {
                truncateDataSize += (UInt64)_prevDataStreamLength[0];
                _prevDataStreamLength.RemoveAt(0);
                var filePath = _dataFilePath + i.ToString();
                File.Delete(filePath);
            }
            _baseFragmentIndex = position.FragmentIndex;
            _fragmentBaseOffset -= truncateDataSize;
            _off -= (int)truncateDataSize;

            foreach(var req in _appendRequestsQueue)
            {
                req.Offset -= (int)truncateDataSize;
            }

            return true;
        }
    }

    public class FilePaxosCommitedDecreeLog : BaseLogWriter, IPaxosCommitedDecreeLog
    {
        private string _dataFilePath;


        public FilePaxosCommitedDecreeLog(string dataFilePath): base(dataFilePath)
        {
            _dataFilePath = dataFilePath;
        }

        public new void Dispose()
        {
            base.Dispose();
        }

        public Task<ICommittedDecreeIterator> Begin()
        {
            var dataStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var fileIt = new FileCommittedDecreeIterator(dataStream, 0, null, IteratorType.Default);
            return fileIt.Next();
        }

        public Task<ICommittedDecreeIterator> End()
        {
            return Task.FromResult(new FileCommittedDecreeIterator(null, 0, null, IteratorType.End) as ICommittedDecreeIterator);
        }

        public async Task<AppendPosition> AppendLog(ulong decreeNo, PaxosDecree decree)
        {
            var begin = DateTime.Now;
            var data = Encoding.UTF8.GetBytes(Serializer<PaxosDecree>.Serialize(decree));

            var position = await AppendLog(BitConverter.GetBytes((uint)data.Length + sizeof(ulong)),
                BitConverter.GetBytes(decreeNo), data);


            var appendTime = DateTime.Now - begin;
            if (appendTime.TotalMilliseconds > 500)
            {
                //Console.WriteLine("too slow");
            }
            return position;
        }


        public new Task Truncate(AppendPosition position)
        {
            return base.Truncate(position);
        }

    }

    class FileVotedBallotIterator : IVotedBallotIterator
    {
        private FileStream _dataStream;
        private IteratorType _itType = IteratorType.Default;
        private Tuple<ulong, DecreeBallotInfo> _votedBallot;

        public FileVotedBallotIterator(
            FileStream dataStream,
            Tuple<ulong, DecreeBallotInfo> votedBallot,
            IteratorType itType)
        {
            _dataStream = dataStream;
            _votedBallot = votedBallot;
            _itType = itType;
        }
        public virtual void Dispose()
        {
        }

        public Tuple<ulong, DecreeBallotInfo> VotedBallot => _votedBallot;

        public async Task<IVotedBallotIterator> Next()
        {
            FileVotedBallotIterator next = null;

            var databuf = new byte[sizeof(uint)];
            var readlen = await _dataStream.ReadAsync(databuf, 0, sizeof(uint));
            if (readlen != sizeof(uint))
            {
                // not enough data
                _dataStream.Close();
                return new FileVotedBallotIterator(null, null, IteratorType.End);
            }
            uint dataSize = BitConverter.ToUInt32(databuf, 0);

            databuf = new byte[dataSize];
            readlen = await _dataStream.ReadAsync(databuf, 0, (int)dataSize);
            if (readlen != dataSize)
            {
                // not enough data
                _dataStream.Close();
                return new FileVotedBallotIterator(null, null, IteratorType.End);
            }
            ulong decreeNo = BitConverter.ToUInt64(databuf, 0);
            var data = Encoding.UTF8.GetString(databuf, (int)sizeof(ulong), (int)(dataSize - sizeof(ulong)));
            var decreeBallotInfo = Serializer<DecreeBallotInfo>.Deserialize(data);

            next = new FileVotedBallotIterator(
                _dataStream,
                new Tuple<ulong, DecreeBallotInfo>(decreeNo, decreeBallotInfo),
                IteratorType.Default);

            return next;
        }

        public bool Equals(IVotedBallotIterator rhs)
        {
            var rhsFileIt = rhs as FileVotedBallotIterator;
            if (rhsFileIt == null)
            {
                return false;
            }
            if (_itType == rhsFileIt._itType && _itType == IteratorType.End)
            {
                return true;
            }
            if (_itType != rhsFileIt._itType)
            {
                return false;
            }
            if (_dataStream != rhsFileIt._dataStream)
            {
                return false;
            }
            if (_dataStream.Position != rhsFileIt._dataStream.Position)
            {
                return false;
            }
            if (_votedBallot.Item1 != rhsFileIt._votedBallot.Item1)
            {
                return false;
            }
            if (!_votedBallot.Item2.Equals(rhsFileIt._votedBallot.Item2))
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            var rhs = obj as IVotedBallotIterator;
            return Equals(rhs);
        }

        public override int GetHashCode()
        {
            return 1;
        }
    }

    public class FilePaxosVotedBallotLog : BaseLogWriter, IPaxosVotedBallotLog
    {
        private string _dataFilePath;

        public FilePaxosVotedBallotLog(string dataFilePath) : base(dataFilePath)
        {
            _dataFilePath = dataFilePath;
        }
        public new void Dispose()
        {
            base.Dispose();
        }

        public Task<IVotedBallotIterator> Begin()
        {
            var dataStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var fileIt = new FileVotedBallotIterator(dataStream, null, IteratorType.Default);
            return fileIt.Next();
        }

        public Task<IVotedBallotIterator> End()
        {
            return Task.FromResult(new FileVotedBallotIterator(null, new Tuple<ulong, DecreeBallotInfo>(0, null), IteratorType.End) as IVotedBallotIterator);
        }


        public async Task<AppendPosition> AppendLog(ulong decreeNo, DecreeBallotInfo ballotInfo)
        {
            var begin = DateTime.Now;
            var data = Encoding.UTF8.GetBytes(Serializer<DecreeBallotInfo>.Serialize(ballotInfo));

            var position = await AppendLog(BitConverter.GetBytes((uint)data.Length + sizeof(ulong)),
                BitConverter.GetBytes(decreeNo), data);

            var totalTime = DateTime.Now - begin;
            if (totalTime.TotalMilliseconds > 500)
            {
                //Console.WriteLine("too slow");
            }

            return position;
        }

    }

    public interface IPaxosNotePeisisent
    {
        Task SaveVoterVoteInfo(ulong decreeNo, DecreeBallotInfo decreeBallotInfo);
        Task<DecreeBallotInfo> LoadVoterVoteInfo(ulong decreeNo);
    }


    public class MemoryPaxosNotePersistent : IPaxosNotePeisisent
    {
        private ConcurrentDictionary<ulong, DecreeBallotInfo> _ballotInfoTable = new ConcurrentDictionary<ulong, DecreeBallotInfo>();
        public MemoryPaxosNotePersistent()
        { }

        public Task SaveVoterVoteInfo(ulong decreeNo, DecreeBallotInfo decreeBallotInfo)
        {
            _ballotInfoTable.AddOrUpdate(decreeNo, decreeBallotInfo, (key, oldValue) => decreeBallotInfo);
            return Task.CompletedTask;
        }

        public Task<DecreeBallotInfo> LoadVoterVoteInfo(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = null;
            if (_ballotInfoTable.TryGetValue(decreeNo, out decreeBallotInfo))
            {
                return Task<DecreeBallotInfo>.FromResult(decreeBallotInfo);
            }
            return Task<DecreeBallotInfo>.FromResult(decreeBallotInfo);
        }

    }

}
