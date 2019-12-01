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

    public class DecreeReadResult
    {
        public bool IsFound { get; set; }
        public ulong MaxDecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
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
                    VoteDecree = commitedDecree,
                    CommittedDecrees = _ledger.GetFollowingCommittedDecress(msg.DecreeNo)
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

        public Task<ProposeResult> BeginNewPropose(PaxosDecree decree)
        {
            var completionSource = new TaskCompletionSource<ProposeResult>();
            Action action = async () =>
            {
                do
                {
                    var oneInstanceCompletionSource = new TaskCompletionSource<ProposeResult>();
                    await ProcessBeginNewBallotRequest(decree, oneInstanceCompletionSource);
                    var result = await oneInstanceCompletionSource.Task;
                    if (result.Decree.Content.Equals(decree.Content))
                    {
                        completionSource.SetResult(result);
                        break;
                    }
                } while (true);
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

        private Task ProcessBeginNewBallotRequest(
            PaxosDecree decree,
            TaskCompletionSource<ProposeResult> proposeCompletionNotification)
        {
            ulong nextDecreeNo = _proposerNote.GetNewDecreeNo();

            //
            // several propose may happen concurrently. All of them will
            // get a unique decree no.
            //

            // never have chance propose a committed decree since every time
            // new decree no will be got

            /*
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
            }*/

            _proposerNote.SubscribeCompletionNotification(nextDecreeNo, proposeCompletionNotification);

            var ballotNo = _proposerNote.PrepareNewBallot(nextDecreeNo, decree);
            if (ballotNo == 0)
            {
                // cant be, send alert
            }

            QueryLastVote(nextDecreeNo, ballotNo);

            return Task.CompletedTask;
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

                _nodeTalkChannel.SendMessage(nextBallotMessage);
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
                _nodeTalkChannel.SendMessage(beginBallotMessage);
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
                await _nodeTalkChannel.SendMessage(successMessage);
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
            var proposerNote = new ProposerNote(ledger);
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, _nodeTalkChannel, proposerNote, ledger);
        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //

           return  _proposerRole.BeginNewPropose(decree);
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            var result = await _proposerRole.ReadDecree(decreeNo);
            return result;
        }


        ///
        /// Following are messages channel, should be moved out of the node interface
        ///
        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>

        public async Task DeliverMessage(PaxosMessage message)
        {
            switch (message.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    await ProcessNextBallot(message as NextBallotMessage);
                    break;
                case PaxosMessageType.LASTVOTE:
                    ProcessLastVote(message as LastVoteMessage);
                    break;
                case PaxosMessageType.BEGINBALLOT:
                    await ProcessBeginBallot(message as BeginBallotMessage);
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

        private async Task ProcessNextBallot(NextBallotMessage msg)
        {
            await _voterRole.DeliverNextBallotMessage(msg);
        }
        private void ProcessLastVote(LastVoteMessage msg)
        {
            _proposerRole.DeliverLastVoteMessage(msg);
        }
        private async Task ProcessBeginBallot(BeginBallotMessage msg)
        {
            await _voterRole.DeliverBeginBallotMessage(msg);
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
