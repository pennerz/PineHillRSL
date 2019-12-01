using Paxos.Message;
using Paxos.Persistence;
using Paxos.Request;
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

        public Dictionary<ulong, PaxosDecree> GetFollowingCommittedDecress(ulong beginDecreeNo)
        {
            var committedDecrees = new Dictionary<ulong, PaxosDecree>();
            for (ulong decreeNo = beginDecreeNo; decreeNo <= GetMaxCommittedDecreeNo(); decreeNo++)
            {
                PaxosDecree committedDecree = null;
                if (_committedDecrees.TryGetValue(decreeNo, out committedDecree))
                {
                    committedDecrees.Add(decreeNo, committedDecree);
                }
            }

            return committedDecrees.Count > 0 ? committedDecrees : null;
        }

        public Task CommitDecree(ulong decreeNo, PaxosDecree decree)
        {
            lock (_committedDecrees)
            {
                do
                {
                    if (_committedDecrees.ContainsKey(decreeNo))
                    {
                        return Task.CompletedTask;
                        //throw new Exception("decree already committed");
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

    public class Propose
    {
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        private readonly List<LastVoteMessage> _lastVoteMessages = new List<LastVoteMessage>();
        private readonly List<VoteMessage> _voteMessages = new List<VoteMessage>();

        public Propose()
        {
            State = PropserState.Init;
            LastTriedBallot = 0;
            OngoingDecree = null;
        }

        public Task AcquireLock()
        {
            return _lock.WaitAsync();
        }

        public void ReleaseLock()
        {
            _lock.Release();
        }

        public void SubscribeCompletionNotification(TaskCompletionSource<ProposeResult> completionSource)
        {
            lock(_subscribedCompletionSource)
            {
                if (completionSource != null)
                {
                    _subscribedCompletionSource.Add(completionSource);
                }
            }
        }

        public ulong PrepareNewBallot(PaxosDecree decree)
        {
            // lastvote message, vote message can touch the propose's state
            // lock it
            lock(_subscribedCompletionSource)
            {
                LastTriedBallot++;
                OngoingDecree = decree;
                _lastVoteMessages.Clear();
                _voteMessages.Clear();
                State = PropserState.QueryLastVote;
                return LastTriedBallot;
            }
        }

        public bool BeginCommit(ulong decreeNo, ulong ballotNo, PaxosDecree committingDecree)
        {
            lock(_subscribedCompletionSource)
            {
                if (ballotNo != LastTriedBallot)
                {
                    // stale commit
                    return false;
                }
                OngoingDecree = committingDecree;
                State = PropserState.BeginCommit;
                return true;
            }
        }

        public ulong AddLastVoteMessage(LastVoteMessage lastVoteMsg)
        {
            lock(_subscribedCompletionSource)
            {
                LastVoteMessages.Add(lastVoteMsg);

                return (ulong)LastVoteMessages.Count;
            }
        }

        public PaxosDecree BeginNewBallot(ulong ballotNo)
        {
            lock(_subscribedCompletionSource)
            {
                if (LastTriedBallot != ballotNo)
                {
                    // last tried ballot no not match
                    return null;
                }
                var maxVoteMsg = GetMaximumVote();
                if (maxVoteMsg != null)
                {
                    OngoingDecree = maxVoteMsg.VoteDecree;
                }
                State = PropserState.BeginNewBallot;

                return OngoingDecree;
            }
        }

        public ulong AddVoteMessage(VoteMessage voteMsg)
        {
            lock (_subscribedCompletionSource)
            {
                VotedMessages.Add(voteMsg);

                return (ulong)VotedMessages.Count;
            }
        }


        private LastVoteMessage GetMaximumVote()
        {
            LastVoteMessage maxVoteMsg = null;
            foreach (var voteMsg in LastVoteMessages)
            {
                if (maxVoteMsg == null || voteMsg.VoteBallotNo > maxVoteMsg.VoteBallotNo)
                {
                    maxVoteMsg = voteMsg;
                }
            }

            if (maxVoteMsg == null)
            {
                return null;
            }

            if (maxVoteMsg.VoteBallotNo > 0)
            {
                return maxVoteMsg;
            }
            return null;
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
        private Ledger _ledger = new Ledger();

        public ProposerNote(Ledger otherLedger)
        {
            if (otherLedger != null)
            {
                _ledger = otherLedger;
            }
        }

        public ulong GetNewDecreeNo()
        {
            ulong nextDecreeNo = 0;

            // new decree no generation is syncrhonized
            lock(_ledger)
            {
                nextDecreeNo = _ledger.GetMaxCommittedDecreeNo() + 1;
                var maxOngoingDecreeNo = GetMaxOngoingDecreeNo();
                if (nextDecreeNo < maxOngoingDecreeNo + 1)
                {
                    nextDecreeNo = maxOngoingDecreeNo + 1;
                }
                var propose = AddPropose(nextDecreeNo);
            }

            return nextDecreeNo;
        }

        public ulong GetMaximumCommittedDecreeNo()
        {
            return _ledger.GetMaxCommittedDecreeNo();
        }

        public void PrepareDecreeBallot(ulong decreeNo)
        {
            AddPropose(decreeNo);
        }

        public Task<PaxosDecree> GetCommittedDecree(ulong decreeNo)
        {
            // committed dcree can never be changed
            return Task.FromResult(_ledger.GetCommittedDecree(decreeNo));
            /*
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return null;
            }
            await propose.AcquireLock();
            committedDecree = _ledger.GetCommittedDecree(decreeNo);
            propose.ReleaseLock();
            return committedDecree;*/
        }

        public void SubscribeCompletionNotification(ulong decreeNo, TaskCompletionSource<ProposeResult> completionSource)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return;
            }
            propose.SubscribeCompletionNotification(completionSource);
        }

        public ulong PrepareNewBallot(ulong decreeNo, PaxosDecree decree)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return 0;
            }
            return propose.PrepareNewBallot(decree);
        }

        public bool BeginCommit(ulong decreeNo, ulong ballotNo, PaxosDecree committingDecree)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return false;
            }
            return propose.BeginCommit(decreeNo, ballotNo, committingDecree);
        }

        public ulong  AddLastVoteMessage(ulong decreeNo, LastVoteMessage lastVoteMsg)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return 0;
            }

            return propose.AddLastVoteMessage(lastVoteMsg);
        }

        public PaxosDecree BeginNewBallot(ulong decreeNo, ulong ballotNo)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return null;
            }

            return propose.BeginNewBallot(ballotNo);
        }

        public ulong AddVoteMessage(ulong decreeNo, VoteMessage voteMsg)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return 0;
            }

            return propose.AddVoteMessage(voteMsg);
        }

        public async Task<Propose> Commit(ulong decreeNo)
        {
            var propose = GetPropose(decreeNo);
            if (propose == null)
            {
                return null;
            }

            // write the decree to ledge
            await propose.AcquireLock();
            var commitedDecree = propose.OngoingDecree;
            if (commitedDecree == null)
            {
                propose.ReleaseLock();
                return null;
            }

            await _ledger.CommitDecree(decreeNo, commitedDecree);

            propose.State = PropserState.Commited; // committed

            // propose commited, remove it from ongoing proposes

            while (!_decreeState.TryRemove(decreeNo, out propose)) ;

            propose.ReleaseLock();

            return propose;
        }


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
