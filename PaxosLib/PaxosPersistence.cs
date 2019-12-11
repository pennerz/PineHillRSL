using Paxos.Notebook;
using Paxos.Request;
using Paxos.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Persistence
{
    public enum IteratorType { Default, End};

    public interface ICommittedDecreeIterator : IEquatable<ICommittedDecreeIterator>
    {
        PaxosDecree Decree { get; }
        Task<ICommittedDecreeIterator> Next();
    }

    public interface IPaxosCommitedDecreeLog
    {
        Task<ICommittedDecreeIterator> Begin();
        Task<ICommittedDecreeIterator> End();
        Task AppendLog(ulong decreeNo, PaxosDecree decree);
    }

    public interface IVotedBallotIterator : IEquatable<IVotedBallotIterator>
    {
        Tuple<ulong, DecreeBallotInfo> VotedBallot { get; }
        Task<IVotedBallotIterator> Next();
    }

    public interface IPaxosVotedBallotLog
    {
        Task<IVotedBallotIterator> Begin();
        Task<IVotedBallotIterator> End();

        Task AppendLog(ulong decreeNo, DecreeBallotInfo decreeBallotInfo);
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
        private IteratorType _itType = IteratorType.Default;
        public FileCommittedDecreeIterator(
            FileStream dataStream,
            PaxosDecree decree, IteratorType itType)
        {
            _dataStream = dataStream;
            _currentDecree = decree;
            _itType = itType;
        }
        ~FileCommittedDecreeIterator()
        {
            _dataStream?.Close();
        }
        public PaxosDecree Decree => _currentDecree;

        public async Task<ICommittedDecreeIterator> Next()
        {
            FileCommittedDecreeIterator next = null;

            var databuf = new byte[sizeof(uint)];
            var readlen = await _dataStream.ReadAsync(databuf, 0, sizeof(uint));
            if (readlen != sizeof(uint))
            {
                // not enough data
                _dataStream.Close();
                return new FileCommittedDecreeIterator(null, null, IteratorType.End);
            }
            uint dataSize = BitConverter.ToUInt32(databuf, 0);
            databuf = new byte[dataSize];
            readlen = await _dataStream.ReadAsync(databuf, 0, sizeof(uint));
            if (readlen != dataSize)
            {
                // not enough data
                _dataStream.Close();
                return new FileCommittedDecreeIterator(null, null, IteratorType.End);
            }

            ulong decreeNo = BitConverter.ToUInt64(databuf, 0);
            var data = Encoding.UTF8.GetString(databuf, (int)sizeof(ulong), (int)(dataSize - sizeof(ulong)));
            var decree = Serializer<PaxosDecree>.Deserialize(data);

            next = new FileCommittedDecreeIterator(_dataStream, decree, IteratorType.Default);

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

    public class FilePaxosCommitedDecreeLog : IPaxosCommitedDecreeLog
    {
        private string _dataFilePath;
        private FileStream _dataStream;

        public FilePaxosCommitedDecreeLog(string dataFilePath)
        {
            _dataFilePath = dataFilePath;
        }

        public Task<ICommittedDecreeIterator> Begin()
        {
            var dataStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
            var fileIt = new FileCommittedDecreeIterator(dataStream, null, IteratorType.Default);
            return fileIt.Next();
        }

        public Task<ICommittedDecreeIterator> End()
        {
            return Task.FromResult(new FileCommittedDecreeIterator(null, null, IteratorType.End) as ICommittedDecreeIterator);
        }

        public void OpenForAppend()
        {
            _dataStream?.Close();
            _dataStream = new FileStream(_dataFilePath, FileMode.OpenOrCreate, FileAccess.Write);
        }

        public async Task AppendLog(ulong decreeNo, PaxosDecree decree)
        {
            lock(_dataFilePath)
            {
                if (_dataStream == null)
                {
                    OpenForAppend();
                }
            }

            var data = Encoding.UTF8.GetBytes(Serializer<PaxosDecree>.Serialize(decree));
            var dataBuf = new byte[sizeof(uint) + sizeof(ulong) + data.Length];
            data.CopyTo(dataBuf, sizeof(uint) + sizeof(ulong));
            BitConverter.GetBytes((uint)data.Length + sizeof(ulong)).CopyTo(dataBuf, 0);
            BitConverter.GetBytes(decreeNo).CopyTo(dataBuf, sizeof(uint));
            await _dataStream.WriteAsync(dataBuf, 0, dataBuf.Length);

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
        ~FileVotedBallotIterator()
        {
            _dataStream?.Close();
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
             readlen = await _dataStream.ReadAsync(databuf, 0, sizeof(ulong));
            if (readlen != sizeof(uint))
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

    public class FilePaxosVotedBallotLog : IPaxosVotedBallotLog
    {
        private string _dataFilePath;
        private FileStream _dataStream;

        public FilePaxosVotedBallotLog(string dataFilePath)
        {
            _dataFilePath = dataFilePath;
        }

        public Task<IVotedBallotIterator> Begin()
        {
            var dataStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
            var fileIt = new FileVotedBallotIterator(dataStream, null, IteratorType.Default);
            return fileIt.Next();
        }

        public Task<IVotedBallotIterator> End()
        {
            return Task.FromResult(new FileVotedBallotIterator(null, new Tuple<ulong, DecreeBallotInfo>(0, null), IteratorType.End) as IVotedBallotIterator);
        }

        public void OpenForAppend()
        {
            _dataStream?.Close();
            _dataStream = new FileStream(_dataFilePath, FileMode.OpenOrCreate, FileAccess.Write);
        }

        public async Task AppendLog(ulong decreeNo, DecreeBallotInfo ballotInfo)
        {
            lock (_dataFilePath)
            {
                if (_dataStream == null)
                {
                    OpenForAppend();
                }
            }
            var data = Encoding.UTF8.GetBytes(Serializer<DecreeBallotInfo>.Serialize(ballotInfo));
            var dataBuf = new byte[sizeof(uint) + sizeof(ulong) + data.Length];
            data.CopyTo(dataBuf, sizeof(uint) + sizeof(ulong) );
            BitConverter.GetBytes((uint)data.Length + sizeof(ulong)).CopyTo(dataBuf, 0);
            BitConverter.GetBytes(decreeNo).CopyTo(dataBuf, sizeof(uint));
            await _dataStream.WriteAsync(dataBuf, 0, dataBuf.Length);
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
