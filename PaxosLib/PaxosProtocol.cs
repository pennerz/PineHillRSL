using Paxos.Network;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Persistence;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Linq;

namespace Paxos.Protocol
{

    public interface IPaxos
    {
        Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree);
        Task Checkpoint();
    }

    public interface IPaxosStateMachine
    {
    }

    public class PaxosCluster
    {
        private List<NodeInfo> members = new List<NodeInfo>();

        public List<NodeInfo> Members
        {
            get
            {
                return members;
            }
        }
    }

    /// <summary>
    /// Event Received          Action
    /// LastVote                Return last vote if the new ballot no bigger than NextBallotNo recorded
    /// BeginNewBallot          If the new ballotNo is equal to NextBallotNo recorded,vote for the ballot
    ///                         and save the vote as last vote
    /// CommitDecree            Save the decree
    /// </summary>
    ///
    public class VoterRole
    {
        IPaxosNodeTalkChannel _nodeTalkChannel;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly VoterNote _note;
        private readonly Ledger _ledger;

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IPaxosNodeTalkChannel talkChannel,
            VoterNote paxoserNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (talkChannel == null) throw new ArgumentNullException("IPaxosNodeTalkChannel");
            if (paxoserNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _nodeTalkChannel = talkChannel;
            _note = paxoserNote;
            _ledger = ledger;
        }

        public async Task DeliverNextBallotMessage(NextBallotMessage msg)
        {
            // check if the decree committed
            LastVoteMessage lastVoteMsg;
            var commitedDecree = _ledger.GetCommittedDecree(msg.DecreeNo);
            if (commitedDecree != null)
            {
                lastVoteMsg = new LastVoteMessage()
                {
                    TargetNode = msg.SourceNode,
                    Commited = true,
                    BallotNo = msg.BallotNo,
                    DecreeNo = msg.DecreeNo,
                    VoteBallotNo = 0, // not applicable
                    VoteDecree = commitedDecree
                };

                await _nodeTalkChannel.SendMessage(lastVoteMsg);
                return;
            }

            var result = await _note.UpdateNextBallotNo(msg.DecreeNo, msg.BallotNo);
            var oldBallotNo = result.Item1;
            var lastVote = result.Item2;
            if (oldBallotNo >= msg.BallotNo)
            {
                // do not response the ballotNo < current nextBallotNo
                var staleBallotMsg = new StaleBallotMessage()
                {
                    NextBallotNo = oldBallotNo
                };
                staleBallotMsg.TargetNode = msg.SourceNode;
                staleBallotMsg.BallotNo = msg.BallotNo;
                staleBallotMsg.DecreeNo = msg.DecreeNo;

                await _nodeTalkChannel.SendMessage(staleBallotMsg);

                return;
            }

            // it could be possible when this lastvotemessage is sent out
            // a new ballot has been voted, in that case, when this LastVoteMessage
            // returned, two case
            // 1. the new ballot is triggered by same node as this one, in this case
            //    this LastVoteMessage will be abandoned
            // 2. the new ballot is triggered by other node, in this case, the node
            //    trigger this ballot will accept this last LastVoteMessage, and it

            // send back the last vote information
            lastVoteMsg = new LastVoteMessage();
            lastVoteMsg.TargetNode = msg.SourceNode;
            lastVoteMsg.BallotNo = msg.BallotNo;
            lastVoteMsg.DecreeNo = msg.DecreeNo;
            lastVoteMsg.VoteBallotNo = lastVote != null ? lastVote.BallotNo : 0;
            lastVoteMsg.VoteDecree = lastVote?.VoteDecree;

            await _nodeTalkChannel.SendMessage(lastVoteMsg);
        }

        public async Task DeliverBeginBallotMessage(BeginBallotMessage msg)
        {
            if (_ledger.GetCommittedDecree(msg.DecreeNo) != null)
            {
                return;
            }

            ulong nextBallotNo = _note.GetNextBallotNo(msg.DecreeNo);
            if (msg.BallotNo > nextBallotNo)
            {
                // should not happend, send alert
                return;
            }

            if (msg.BallotNo < nextBallotNo)
            {
            }

            // vote this ballot
            var voteMsg = new VoteMessage();
            voteMsg.TargetNode = msg.SourceNode;
            voteMsg.BallotNo = msg.BallotNo;
            voteMsg.DecreeNo = msg.DecreeNo;
            voteMsg.VoteDecree = msg.Decree;

            // save last vote
            var oldNextBallotNo = await _note.UpdateLastVote(msg.DecreeNo, msg.BallotNo, voteMsg);
            if (oldNextBallotNo > msg.BallotNo)
            {
                var staleBallotMsg = new StaleBallotMessage()
                {
                    NextBallotNo = nextBallotNo
                };
                staleBallotMsg.TargetNode = msg.SourceNode;
                staleBallotMsg.BallotNo = msg.BallotNo;
                staleBallotMsg.DecreeNo = msg.DecreeNo;

                await _nodeTalkChannel.SendMessage(staleBallotMsg);

                return;
            }
            if (oldNextBallotNo < msg.BallotNo)
            {
                // cant be, send alert
                return;
            }

            // deliver the vote message
            await _nodeTalkChannel.SendMessage(voteMsg);
        }

        public void DeliverSuccessMessage(SuccessMessage msg)
        {
            // save it to ledge
            _ledger.CommitDecree(msg.DecreeNo, msg.Decree);
            //_note.ClearDecree(msg.DecreeNo);
        }
    }

    /// <summary>
    /// Three phase commit (3PC)
    /// 1. query existing vote
    /// 2. prepare commit
    /// 3. commit
    /// 
    ///     CurrentState            Event                   NextState               Action
    ///     Init                    Propose                 QueryLastVote           Query lastvote from cluster
    ///     QueryLastVote           ReceiveLastVote         BeginNewBallot(enough)  Send BeginNewBallot to quorum
    ///                                                     QueryLastVote(x)        Nothing
    ///                                                     BeginCommit(Committed)   Send commit to missing decree node
    ///     QueryLastVote           Timeout                 QueryLastVote           Query lastvote with new ballotNo
    ///     BeginNewBallot          ReceiveVote             BeginCommit(enough)     Send commit to all nodes
    ///                                                     ReceiveVote(x)          Nothing
    ///     BeginNewBallot          Timeout                 QueryLastVote           Send BeginNewBallot with a new ballotNo
    ///     BeginCommit             Response                Committed(enough)        Call subscriber
    ///                                                     BeginCommit(x)
    ///     BeginCommit             Timeout                 BeginCommit             Send commit to all nodes
    ///     
    /// </summary>

    public class ProposerRole
    {
        IPaxosNodeTalkChannel _nodeTalkChannel;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly Ledger _ledger;

        private List<Task> _tasksQueue = new List<Task>();
        private Task _messageHandlerTask;
        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IPaxosNodeTalkChannel talkChannel,
            ProposerNote proposerNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (talkChannel == null) throw new ArgumentNullException("IPaxosNodeTalkChannel");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _nodeTalkChannel = talkChannel;
            _proposerNote = proposerNote;
            _ledger = ledger;

            Stop = false;

            _messageHandlerTask = Task.Run(async () =>
            {
                while(!Stop)
                {
                    if (_tasksQueue.Count == 0)
                    {
                        await Task.Delay(100);
                        continue;
                    }

                    var task = _tasksQueue[0];
                    _tasksQueue.RemoveAt(0);
                    task.Start();
                    await task;
                }
            });
        }

        public bool Stop { get; set; }

        public Task<ProposeResult> BeginNewPropose(PaxosDecree decree, ulong nextDecreeNo)
        {
            var completionSource = new TaskCompletionSource<ProposeResult>();
            Action action = () =>
            {
                ProcessBeginNewBallotRequest(decree, nextDecreeNo, completionSource);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = () =>
            {
                ProcessStaleBallotMessage(msg, completionSource);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverLastVoteMessage(LastVoteMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = () =>
            {
                ProcessLastVoteMessage(msg, completionSource);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverVoteMessage(VoteMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = () =>
            {
                ProcessVoteMessage(msg, completionSource);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        private void ProcessBeginNewBallotRequest(PaxosDecree decree, ulong nextDecreeNo, TaskCompletionSource<ProposeResult> completionSource)
        {
            if (nextDecreeNo == 0)
            {
                nextDecreeNo = _ledger.GetMaxCommittedDecreeNo() + 1;
                var maxOngoingDecreeNo = _proposerNote.GetMaxOngoingDecreeNo();
                if (nextDecreeNo < maxOngoingDecreeNo + 1)
                {
                    nextDecreeNo = maxOngoingDecreeNo + 1;
                }
            }

            var committedDecree = _ledger.GetCommittedDecree(nextDecreeNo);
            if (committedDecree != null)
            {
                // already committed, return directly
                var result = new ProposeResult()
                {
                    DecreeNo = nextDecreeNo,
                    Decree = committedDecree
                };

                completionSource?.SetResult(result);
                return;
            }

            var propose = _proposerNote.AddPropose(nextDecreeNo);

            propose.LastTriedBallot++;
            propose.OngoingDecree = decree;
            propose.CompletionEvents.Add(completionSource);
            propose.State = PropserState.QueryLastVote;

            QueryLastVote(nextDecreeNo, propose.LastTriedBallot);
        }

        private void ProcessStaleBallotMessage(StaleBallotMessage msg, TaskCompletionSource<bool> completionSource)
        {
            // QueryLastVote || BeginNewBallot
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                completionSource.SetResult(false);
                return;
            }
            if (propose.State != PropserState.QueryLastVote && propose.State != PropserState.BeginNewBallot)
            {
                completionSource.SetResult(false);
                return;
            }

            if (propose.LastTriedBallot != msg.BallotNo)
            {
                completionSource.SetResult(false);
                return;
            }

            propose.LastVoteMessages.Clear();
            propose.VotedMessages.Clear();

            // query last vote again
            ulong nextBallotNo = msg.NextBallotNo + 1;
            propose.LastTriedBallot = nextBallotNo;

            propose.State = PropserState.QueryLastVote;

            _proposerNote.UpdatePropose(msg.DecreeNo, propose); // currently not needed acturally

            QueryLastVote(msg.DecreeNo, nextBallotNo);

            completionSource.SetResult(true);
        }

        private void ProcessLastVoteMessage(LastVoteMessage msg, TaskCompletionSource<bool> completionSource)
        {
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                completionSource.SetResult(false);
                return;
            }
            if (propose.State != PropserState.QueryLastVote)
            {
                completionSource.SetResult(false);
                return;
            }

            // LastTriedBallot 
            if (propose.LastTriedBallot != msg.BallotNo)
            {
                completionSource.SetResult(false);
                return;
            }

            // cant not go to the following path, either proposer's note has no information
            // about the decree, or the state in the proposer's note already comes to committed
            // which will return at the first condition check
            //if (_ledger.GetCommittedDecree(msg.DecreeNo) != null)
            //{
            //    // already committed
            //    completionSource.SetResult(false);
            //    return;
            //}

            if (msg.Commited)
            {
                // decree already committed
                propose.OngoingDecree = msg.VoteDecree;
                propose.State = PropserState.BeginCommit;
                _proposerNote.UpdatePropose(msg.DecreeNo, propose);

                BeginCommit(msg.DecreeNo);

                completionSource.SetResult(false);
                return;
            }

            // TODO: check if message come from existing node
            propose.LastVoteMessages.Add(msg);

            if (propose.LastVoteMessages.Count >= _cluster.Members.Count / 2 + 1)
            {
                // enough feedback got
                var maxVote = GetMaximumVote(msg.DecreeNo);
                if (maxVote != null)
                {
                    propose.OngoingDecree = maxVote.VoteDecree;
                }

                propose.State = PropserState.BeginNewBallot;

                _proposerNote.UpdatePropose(msg.DecreeNo, propose);
                // begin new ballot
                BeginNewBallot(msg.DecreeNo, msg.BallotNo);
            }

            completionSource.SetResult(true);
        }

        private void ProcessVoteMessage(VoteMessage msg, TaskCompletionSource<bool> completionSource)
        {
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                completionSource.SetResult(false);
                return;
            }
            if (propose.State != PropserState.BeginNewBallot)
            {
                completionSource.SetResult(false);
                return;
            }

            if (propose.LastTriedBallot != msg.BallotNo)
            {
                // not the vote ballot
                completionSource.SetResult(false);
                return;
            }

            // TODO: check if message come from existing node
            propose.VotedMessages.Add(msg);

            if (propose.VotedMessages.Count >= _cluster.Members.Count / 2 + 1)
            {
                // enough feedback got, begin to commit
                propose.State = PropserState.BeginCommit;
                _proposerNote.UpdatePropose(msg.DecreeNo, propose);
                BeginCommit(msg.DecreeNo);
            }
            completionSource.SetResult(true);
        }

        private LastVoteMessage GetMaximumVote(UInt64 decreeNo)
        {
            var propose = _proposerNote.GetPropose(decreeNo);
            if (propose == null)
            {
                return null;
            }

            LastVoteMessage maxVoteMsg = null;
            foreach (var voteMsg in propose.LastVoteMessages)
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

        private void QueryLastVote(UInt64 decreeNo, ulong nextBallotNo)
        {
            // 1. collect decree for this instance, send NextBallotMessage
            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var nextBallotMessage = new NextBallotMessage();
                nextBallotMessage.TargetNode = node.Name;
                nextBallotMessage.DecreeNo = decreeNo;
                nextBallotMessage.BallotNo = nextBallotNo;

                _nodeTalkChannel.SendMessage(nextBallotMessage);
            }

        }
        private void BeginNewBallot(UInt64 decreeNo, UInt64 ballotNo)
        {
            var propose = _proposerNote.GetPropose(decreeNo);
            if (propose == null)
            {
                return;
            }

            var passingDecree = propose.OngoingDecree;
            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var beginBallotMessage = new BeginBallotMessage();
                beginBallotMessage.DecreeNo = decreeNo;
                beginBallotMessage.BallotNo = ballotNo;
                beginBallotMessage.TargetNode = node.Name;
                beginBallotMessage.Decree = passingDecree;
                _nodeTalkChannel.SendMessage(beginBallotMessage);
            }
        }

        private void BeginCommit(UInt64 decreeNo)
        {
            // write the decree to ledge
            var propose = _proposerNote.GetPropose(decreeNo);
            if (propose == null)
            {
                return;
            }
            var commitedDecree = propose.OngoingDecree;
            if (commitedDecree == null)
            {
                return;
            }
            _ledger.CommitDecree(decreeNo, commitedDecree);

            propose.State = PropserState.Commited; // committed
            ulong ballotNo = propose.LastTriedBallot;
            var subscriberList = propose.CompletionEvents;
            //_proposerNote.ClearDecree(decreeNo);

            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var successMessage = new SuccessMessage();
                successMessage.TargetNode = node.Name;
                successMessage.DecreeNo = decreeNo;
                successMessage.BallotNo = ballotNo;
                successMessage.Decree = commitedDecree;
                _nodeTalkChannel.SendMessage(successMessage);
            }

            // TODO: confirm the commit succeeded on other nodes
            var result = new ProposeResult()
            {
                DecreeNo = decreeNo,
                Decree = commitedDecree
            };
            foreach (var completionSource in subscriberList)
            {
                completionSource.SetResult(result);
            }

        }
    }

    public class PaxosNode
    {
        IPaxosNodeTalkChannel _nodeTalkChannel;
        VoterRole _voterRole;
        ProposerRole _proposerRole;

        PaxosCluster _cluster;
        NodeInfo _nodeInfo;


        public PaxosNode(
            IPaxosNodeTalkChannel nodeTalkChannel,
            PaxosCluster cluster,
            NodeInfo nodeInfo)
        {
            if (nodeTalkChannel == null)
            {
                throw new ArgumentNullException("Node talk channel is null");
            }
            if (cluster == null)
            {
                throw new ArgumentNullException("cluster is null");
            }
            if (nodeInfo == null)
            {
                throw new ArgumentNullException("nodeInfo is null");
            }

            _nodeTalkChannel = nodeTalkChannel;
            _cluster = cluster;
            _nodeInfo = nodeInfo;

            var persistenter = new MemoryPaxosNotePersistent();

            var ledger = new Ledger();
            var voterNote = new VoterNote(persistenter);
            _voterRole = new VoterRole(_nodeInfo, _cluster, _nodeTalkChannel, voterNote, ledger);
            var proposerNote = new ProposerNote();
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, _nodeTalkChannel, proposerNote, ledger);
        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //

           return  _proposerRole.BeginNewPropose(decree, decreeNo);
        }

        public void DeliverMessage(PaxosMessage message)
        {
            switch (message.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    ProcessNextBallot(message as NextBallotMessage);
                    break;
                case PaxosMessageType.LASTVOTE:
                    ProcessLastVote(message as LastVoteMessage);
                    break;
                case PaxosMessageType.BEGINBALLOT:
                    ProcessBeginBallot(message as BeginBallotMessage);
                    break;
                case PaxosMessageType.VOTE:
                    ProcessVote(message as VoteMessage);
                    break;
                case PaxosMessageType.SUCCESS:
                    ProcessSuccess(message as SuccessMessage);
                    break;
                case PaxosMessageType.STALEBALLOT:
                    ProcessStaleBallotMessage(message as StaleBallotMessage);
                    break;
                default:
                    break;
            }
        }

        private void ProcessNextBallot(NextBallotMessage msg)
        {
            _voterRole.DeliverNextBallotMessage(msg);
        }
        private void ProcessLastVote(LastVoteMessage msg)
        {
            _proposerRole.DeliverLastVoteMessage(msg);
        }
        private void ProcessBeginBallot(BeginBallotMessage msg)
        {
            _voterRole.DeliverBeginBallotMessage(msg);
        }
        private void ProcessVote(VoteMessage msg)
        {
            _proposerRole.DeliverVoteMessage(msg);
        }
        private void ProcessSuccess(SuccessMessage msg)
        {
            _voterRole.DeliverSuccessMessage(msg);
        }

        private void ProcessStaleBallotMessage(StaleBallotMessage msg)
        {
            _proposerRole.DeliverStaleBallotMessage(msg);
        }

    }

}
