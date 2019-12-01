using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using Paxos.Notebook;

namespace Paxos.Persistence
{
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
