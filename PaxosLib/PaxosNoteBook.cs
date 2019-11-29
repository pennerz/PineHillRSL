using Paxos.Message;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Linq;
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

    public class VoterNote
    {
        // voter role
        private readonly ConcurrentDictionary<ulong, VoteMessage> _voteMessages = new ConcurrentDictionary<ulong, VoteMessage>();
        private readonly ConcurrentDictionary<ulong, ulong> _nextBallot = new ConcurrentDictionary<ulong, ulong>();

        //public IDictionary<ulong, VoteMessage> VotedMessage => _voteMessages;
        //public IDictionary<ulong, ulong> NextBallotNo => _nextBallot;
        public ulong GetNextBallotNo(ulong decreeNo)
        {
            ulong nextBallotNo = 0;
            if (_nextBallot.TryGetValue(decreeNo, out nextBallotNo))
            {
                return nextBallotNo;
            }
            return 0;
        }

        public void UpdateNextBallotNo(ulong decreeNo, ulong nextBallotNo)
        {
            _nextBallot.AddOrUpdate(decreeNo, nextBallotNo, (key, oldValue) => nextBallotNo);
        }

        public VoteMessage GetLastVote(ulong decreeNo)
        {
            VoteMessage voteMsg = null;
            if (_voteMessages.TryGetValue(decreeNo, out voteMsg))
            {
                return voteMsg;
            }
            return null;
        }

        public void UpdateLastVote(ulong decreeNo, VoteMessage voteMsg)
        {
            _voteMessages.AddOrUpdate(decreeNo, voteMsg, (key, oldValue) => voteMsg);
        }

        public void Reset()
        {
            _voteMessages.Clear();
            _nextBallot.Clear();
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

        //public IDictionary<UInt64, List<LastVoteMessage>> LastVoteMessages => _lastVoteMessages;

        //public IDictionary<UInt64, List<VoteMessage>> VoteMessages => _voteMessages;

        //public IDictionary<UInt64, PaxosDecree> OngoingPropose => _ongoingPropose;

        //public IDictionary<UInt64, UInt64> LastTriedBallot => _lastTriedBallot;

        //public IDictionary<ulong, Propose> DecreeState => _decreeState;

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
