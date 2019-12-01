using Paxos.Network;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Persistence;
using Paxos.Node;
using Paxos.Request;
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
        IMessageTransport _messageTransport;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly VoterNote _note;
        private readonly Ledger _ledger;

        public VoterRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IMessageTransport messageTransport,
            VoterNote paxoserNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (messageTransport == null) throw new ArgumentNullException("IMessageTransport");
            if (paxoserNote == null) throw new ArgumentNullException("no note book");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _messageTransport = messageTransport;
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
                    VoteDecree = commitedDecree,
                    CommittedDecrees = _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
                };

                await _messageTransport.SendMessage(lastVoteMsg);
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

                await _messageTransport.SendMessage(staleBallotMsg);

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

            await _messageTransport.SendMessage(lastVoteMsg);
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

                await _messageTransport.SendMessage(staleBallotMsg);

                return;
            }
            if (oldNextBallotNo < msg.BallotNo)
            {
                // cant be, send alert
                return;
            }

            // deliver the vote message
            await _messageTransport.SendMessage(voteMsg);
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
        IMessageTransport _messageTransport;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;
        private readonly Ledger _ledger;

        private List<Task> _tasksQueue = new List<Task>();
        private Task _messageHandlerTask;
        public ProposerRole(
            NodeInfo nodeInfo,
            PaxosCluster cluster,
            IMessageTransport messageTransport,
            ProposerNote proposerNote,
            Ledger ledger)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeInfo");
            if (messageTransport == null) throw new ArgumentNullException("IMessageTransport");
            if (proposerNote == null) throw new ArgumentNullException("proposer note");
            if (ledger == null) throw new ArgumentNullException("ledger");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _messageTransport = messageTransport;
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

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            DecreeReadResult result = null;
            var maximumCommittedDecreeNo = _proposerNote.GetMaximumCommittedDecreeNo();
            if (decreeNo > maximumCommittedDecreeNo)
            {
                 result = new DecreeReadResult()
                {
                    IsFound = false,
                    MaxDecreeNo = maximumCommittedDecreeNo,
                    Decree = null
                };
                return result;
            }

            var decree = await _proposerNote.GetCommittedDecree(decreeNo);
            result = new DecreeReadResult()
            {
                IsFound = true,
                MaxDecreeNo = maximumCommittedDecreeNo,
                Decree = decree
            };

            return result;
        }

        public Task<ProposeResult> BeginNewPropose(PaxosDecree decree, ulong decreeNo)
        {
            var completionSource = new TaskCompletionSource<ProposeResult>();
            Action action = async () =>
            {
                if (decreeNo == 0)
                {
                    // other node may already proposed a same decreeNo
                    // continue untill the decree proposed committed
                    do
                    {
                        var oneInstanceCompletionSource = new TaskCompletionSource<ProposeResult>();
                        await ProcessBeginNewBallotRequest(decree, decreeNo, oneInstanceCompletionSource);
                        var result = await oneInstanceCompletionSource.Task;
                        if (result.Decree.Content.Equals(decree.Content))
                        {
                            completionSource.SetResult(result);
                            break;
                        }
                    } while (true);

                }
                else
                {
                    await ProcessBeginNewBallotRequest(decree, decreeNo, completionSource);
                }
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = () =>
            {
                var ret = ProcessStaleBallotMessage(msg);
                completionSource.SetResult(ret);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverLastVoteMessage(LastVoteMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = async () =>
            {
                var ret = await ProcessLastVoteMessage(msg);
                completionSource.SetResult(ret);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        public Task DeliverVoteMessage(VoteMessage msg)
        {
            var completionSource = new TaskCompletionSource<bool>();
            Action action = async () =>
            {
                var ret = await ProcessVoteMessage(msg);
                completionSource.SetResult(ret);
            };
            _tasksQueue.Add(new Task(action));
            return completionSource.Task;
        }

        private async Task ProcessBeginNewBallotRequest(
            PaxosDecree decree,
            ulong nextDecreeNo,
            TaskCompletionSource<ProposeResult> proposeCompletionNotification)
        {
            if (nextDecreeNo == 0)
            {
                //
                // several propose may happen concurrently. All of them will
                // get a unique decree no.
                //
                nextDecreeNo = _proposerNote.GetNewDecreeNo();
            }


            var committedDecree = await _proposerNote.GetCommittedDecree(nextDecreeNo);
            if (committedDecree != null)
            {
                // already committed, return directly
                var result = new ProposeResult()
                {
                    DecreeNo = nextDecreeNo,
                    Decree = committedDecree
                };

                proposeCompletionNotification?.SetResult(result);
                return;
            }

            _proposerNote.SubscribeCompletionNotification(nextDecreeNo, proposeCompletionNotification);

            var ballotNo = _proposerNote.PrepareNewBallot(nextDecreeNo, decree);
            if (ballotNo == 0)
            {
                // cant be, send alert
            }

            QueryLastVote(nextDecreeNo, ballotNo);

            return;
        }

        private bool ProcessStaleBallotMessage(StaleBallotMessage msg)
        {
            // QueryLastVote || BeginNewBallot

            // in case committed, ongoing propose could be cleaned
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                return false;
            }
            if (propose.State != PropserState.QueryLastVote && propose.State != PropserState.BeginNewBallot)
            {
                return false;
            }

            // other stale message may already started new ballot, abandon this message
            if (propose.LastTriedBallot != msg.BallotNo)
            {
                return false;
            }

            // query last vote again
            var decree = propose.OngoingDecree;

            // can be optimized to get the right ballot no once
            // if concurrently stale message reach here, will git a diffent
            // ballo no, and one will become stale
            ulong nextBallotNo = 0;
            do
            {
                nextBallotNo  = _proposerNote.PrepareNewBallot(msg.DecreeNo, decree);
            } while (nextBallotNo <= msg.NextBallotNo);


            // could be several different LastVote query on different ballot
            QueryLastVote(msg.DecreeNo, nextBallotNo);

            return true;
        }

        private async Task<bool> ProcessLastVoteMessage(LastVoteMessage msg)
        {
            // in case committed, ongoing propose could be cleaned
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                return false;
            }

            //
            // 1. decree passed, and begin to vote the ballot
            // 2. decree committed
            //
            if (propose.State != PropserState.QueryLastVote)
            {
                return false;
            }

            //
            // new ballot intialized, thsi lastvotemessage become stale message 
            //
            if (propose.LastTriedBallot != msg.BallotNo)
            {
                return false;
            }

            if (msg.Commited)
            {
                // decree already committed
                if (!_proposerNote.BeginCommit(msg.DecreeNo, msg.BallotNo, msg.VoteDecree))
                {
                    return false;
                }

                await BeginCommit(msg.DecreeNo, msg.BallotNo);

                return false;
            }

            // TODO: check if message come from existing node
            ulong lstVoteMsgCount = _proposerNote.AddLastVoteMessage(msg.DecreeNo, msg);
            if (lstVoteMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
            {
                // enough feedback got
                var newBallotDecree = _proposerNote.BeginNewBallot(msg.DecreeNo, msg.BallotNo);
                if (newBallotDecree == null)
                {
                    // sombody else is doing the job
                    return false;
                }

                // begin new ballot
                BeginNewBallot(msg.DecreeNo, msg.BallotNo, newBallotDecree);
            }

            return true;
        }

        private async Task<bool> ProcessVoteMessage(VoteMessage msg)
        {
            var propose = _proposerNote.GetPropose(msg.DecreeNo);
            if (propose == null)
            {
                return false;
            }
            if (propose.State != PropserState.BeginNewBallot)
            {
                return false;
            }

            if (propose.LastTriedBallot != msg.BallotNo)
            {
                // not the vote ballot
                return false;
            }

            // TODO: check if message come from existing node
            var votesMsgCount = _proposerNote.AddVoteMessage(msg.DecreeNo, msg);

            if (votesMsgCount >= (ulong)_cluster.Members.Count / 2 + 1)
            {
                // enough feedback got, begin to commit
                if (!_proposerNote.BeginCommit(msg.DecreeNo, msg.BallotNo, msg.VoteDecree))
                {
                    return false;
                }

                await BeginCommit(msg.DecreeNo, msg.BallotNo);
            }
            return true;
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

                _messageTransport.SendMessage(nextBallotMessage);
            }

        }
        private void BeginNewBallot(UInt64 decreeNo, UInt64 ballotNo, PaxosDecree newBallotDecree)
        {
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
                beginBallotMessage.Decree = newBallotDecree;
                _messageTransport.SendMessage(beginBallotMessage);
            }
        }

        private async Task BeginCommit(ulong decreeNo, ulong ballotNo)
        {
            // write the decree to ledge
            var propose = await _proposerNote.Commit(decreeNo);


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
                successMessage.Decree = propose.OngoingDecree;
                await _messageTransport.SendMessage(successMessage);
            }

            // TODO: confirm the commit succeeded on other nodes
            var result = new ProposeResult()
            {
                DecreeNo = decreeNo,
                Decree = propose.OngoingDecree
            };
            foreach (var completionSource in subscriberList)
            {
                completionSource.SetResult(result);
            }

        }
    }

}
