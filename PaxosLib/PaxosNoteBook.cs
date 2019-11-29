using Paxos.Message;
using Paxos.Persistence;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Notebook
{
    public class Ledger
    {
        private readonly ConcurrentDictionary<ulong, PaxosDecree> _committedDecrees = new ConcurrentDictionary<ulong, PaxosDecree>();

        public ulong GetMaxCommittedDecreeNo()
        {
            return _committedDecrees.LastOrDefault().Key;
        }

        public ulong GetCommittedDecreeCount()
        {
            return (ulong)_committedDecrees.Count;
        }

        public PaxosDecree GetCommittedDecree(ulong decreeNo)
        {
            PaxosDecree committedDecree = null;
            if (_committedDecrees.TryGetValue(decreeNo, out committedDecree))
            {
                return committedDecree;
            }
            return null;
        }

        public Task CommitDecree(ulong decreeNo, PaxosDecree decree)
        {
            lock (_committedDecrees)
            {
                do
                {
                    if (_committedDecrees.ContainsKey(decreeNo))
                    {
                        throw new Exception("decree already committed");
                    }
                    if (_committedDecrees.TryAdd(decreeNo, decree))
                    {
                        return Task.CompletedTask;
                    }
                } while (true);
            }
        }

        public void Clear()
        {
            _committedDecrees.Clear();
        }
    }

    public class LockStub
    {

    }

    public class DecreeBallotInfo
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        public DecreeBallotInfo()
        {
            DecreeNo = 0;
            NextBallotNo = 0;
            VotedMessage = null;
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
        public VoteMessage VotedMessage { get; set; }
    }

    public class VoterNote
    {
        // persistent module
        private readonly IPaxosNotePeisisent _persistenter = null;
        // voter role
        private readonly ConcurrentDictionary<ulong, DecreeBallotInfo> _ballotInfo = new ConcurrentDictionary<ulong, DecreeBallotInfo>();

        public VoterNote(IPaxosNotePeisisent persistenter)
        {
            _persistenter = persistenter;
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

        public async Task<Tuple<ulong, VoteMessage>> UpdateNextBallotNo(ulong decreeNo, ulong nextBallotNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            ulong oldNextBallotNo = 0;
            await decreeBallotInfo.AcquireLock();
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            var result = new Tuple<ulong, VoteMessage>(oldNextBallotNo, decreeBallotInfo.VotedMessage);
            if (nextBallotNo <= oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return result;
            }
            decreeBallotInfo.NextBallotNo = nextBallotNo;
            await _persistenter.SaveVoterVoteInfo(decreeNo, decreeBallotInfo);
            decreeBallotInfo.ReleaseLock();
            return result;
        }

        public VoteMessage GetLastVote(ulong decreeNo)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            return decreeBallotInfo.VotedMessage;
        }

        public async Task<ulong> UpdateLastVote(ulong decreeNo, ulong nextBallotNo, VoteMessage voteMsg)
        {
            DecreeBallotInfo decreeBallotInfo = AddDecreeBallotInfo(decreeNo);
            await decreeBallotInfo.AcquireLock();
            decreeBallotInfo.VotedMessage = voteMsg;
            await _persistenter.SaveVoterVoteInfo(decreeNo, decreeBallotInfo);
            decreeBallotInfo.ReleaseLock();

            ulong oldNextBallotNo = 0;
            await decreeBallotInfo.AcquireLock();
            oldNextBallotNo = decreeBallotInfo.NextBallotNo;
            if (nextBallotNo != oldNextBallotNo)
            {
                // copy a decreeBallotInfo
                decreeBallotInfo.ReleaseLock();
                return oldNextBallotNo;
            }
            decreeBallotInfo.VotedMessage = voteMsg;
            await _persistenter.SaveVoterVoteInfo(decreeNo, decreeBallotInfo);
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
    }

    public enum PropserState
    {
        Init,               // init
        QueryLastVote,      // query exiting vote
        BeginNewBallot,     // prepare commit
        BeginCommit,        // commit
        Commited            // done
    }
    public class ProposeResult
    {
        public ulong DecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
    }

    public class Propose
    {
        private readonly List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        private readonly List<LastVoteMessage> _lastVoteMessages = new List<LastVoteMessage>();
        private readonly List<VoteMessage> _voteMessages = new List<VoteMessage>();

        public Propose()
        {
            State = PropserState.Init;
            LastTriedBallot = 0;
            OngoingDecree = null;
        }

        public PropserState State { get; set; }

        public ulong LastTriedBallot { get; set; }

        public PaxosDecree OngoingDecree { get; set; }

        public List<VoteMessage> VotedMessages { get { return _voteMessages; } }

        public List<LastVoteMessage> LastVoteMessages { get { return _lastVoteMessages; } }

        public List<TaskCompletionSource<ProposeResult>> CompletionEvents
        {
            get
            {
                return _subscribedCompletionSource;
            }
        }

    }

    public class ProposerNote
    {
        // proposer role
        private ConcurrentDictionary<ulong, Propose> _decreeState = new ConcurrentDictionary<ulong, Propose>();

        public ulong GetMaxOngoingDecreeNo()
        {
            if (_decreeState.Count == 0)
            {
                return 0;
            }
            return _decreeState.LastOrDefault().Key;
        }

        public Propose GetPropose(ulong decreeNo)
        {
            Propose propose = null;
            if (_decreeState.TryGetValue(decreeNo, out propose))
            {
                return propose;
            }

            return null;
        }

        public Propose AddPropose(ulong decreeNo)
        {
            do
            {
                _decreeState.TryAdd(decreeNo, new Propose());
                var propose = GetPropose(decreeNo);
                if (propose != null)
                {
                    return propose;
                }
            } while (true);
        }

        public void UpdatePropose(ulong decreeNo, Propose propose)
        {
            _decreeState.AddOrUpdate(decreeNo, propose, (key, oldValue) => propose);
        }

        public void Reset()
        {
            _decreeState.Clear();
        }

        public void ClearDecree(ulong decreeNo)
        {
            Propose propose = null;
            _decreeState.Remove(decreeNo, out propose);
        }
    }

}
