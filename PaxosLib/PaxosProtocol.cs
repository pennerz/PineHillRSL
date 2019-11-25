﻿

namespace PaxosLib
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using System.Linq;

    public class PaxosDecree
    {
        public string Content;
    }

    public enum PaxosMessageType
    {
        NEXTBALLOT,
        LASTVOTE,
        BEGINBALLOT,
        VOTE,
        SUCCESS,
        STALEBALLOT
    }
    public class PaxosMessage
    {
        public PaxosMessageType MessageType {get; set;}
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        public UInt64 DecreeNo { get; set; }
        public UInt64 BallotNo { get; set; }
    }

    public class NextBallotMessage : PaxosMessage
    {
        public NextBallotMessage()
        {
            MessageType = PaxosMessageType.NEXTBALLOT;
        }
    }

    public class LastVoteMessage : PaxosMessage
    {
        public LastVoteMessage()
        {
            MessageType = PaxosMessageType.LASTVOTE;
        }

        public ulong VoteBallotNo { get; set; }
        public PaxosDecree VoteDecree { get; set; }
    }

    public class BeginBallotMessage : PaxosMessage
    {
        public BeginBallotMessage()
        {
            MessageType = PaxosMessageType.BEGINBALLOT;
        }
        public PaxosDecree Decree { get; set; }
    }

    public class VoteMessage : PaxosMessage
    {
        public VoteMessage()
        {
            MessageType = PaxosMessageType.VOTE;
        }
        public PaxosDecree VoteDecree { get; set; }
    }

    public class SuccessMessage : PaxosMessage
    {
        public SuccessMessage()
        {
            MessageType = PaxosMessageType.SUCCESS;
        }
        public PaxosDecree Decree { get; set; }
    }

    public class StaleBallotMessage : PaxosMessage
    {
        public StaleBallotMessage()
        {
            MessageType = PaxosMessageType.STALEBALLOT;
        }

        public ulong NextBallotNo { get; set; }
    }

    public interface IPaxos
    {
        Task UpdateSuccessfullDecree(UInt64 decreeNo, PaxosDecree decree);
        Task Checkpoint();
    }

    public interface IPaxosStateMachine
    {
    }
    public interface IPaxosNodeTalkChannel
    {
        Task SendMessage(PaxosMessage msg);
    }
    public class NodeInfo
    {
        public string Name { get; set; }

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

    public class Ledger
    {
        public UInt64 NextBallot { get; set; }
        public UInt64 LastVote { get; set; }

    }

    public class VoterNote
    {
        // voter role
        private readonly Dictionary<ulong, VoteMessage> _voteMessages = new Dictionary<ulong, VoteMessage>();
        private readonly Dictionary<ulong, ulong> _nextBallot = new Dictionary<ulong, ulong>();

        public Dictionary<ulong, VoteMessage> VotedMessage => _voteMessages;
        public Dictionary<ulong, ulong> NextBallotNo => _nextBallot;

        public void Reset()
        {
            _voteMessages.Clear();
            _nextBallot.Clear();
        }
    }

    public class ProposerNote
    {
        // proposer role
        private readonly Dictionary<UInt64, List<LastVoteMessage>> _lastVoteMessages = new Dictionary<UInt64, List<LastVoteMessage>>();
        private readonly Dictionary<UInt64, List<VoteMessage>> _voteMessages = new Dictionary<UInt64, List<VoteMessage>>();
        private readonly Dictionary<UInt64, PaxosDecree> _ongoingPropose = new Dictionary<ulong, PaxosDecree>();

        private Dictionary<UInt64, UInt64> _lastTriedBallot = new Dictionary<ulong, ulong>();
        private Dictionary<ulong, Propose> _decreeState = new Dictionary<ulong, Propose>();

        public Dictionary<UInt64, List<LastVoteMessage>> LastVoteMessages => _lastVoteMessages;

        public Dictionary<UInt64, List<VoteMessage>> VoteMessages => _voteMessages;

        public Dictionary<UInt64, PaxosDecree> OngoingPropose => _ongoingPropose;

        public Dictionary<UInt64, UInt64> LastTriedBallot => _lastTriedBallot;

        public Dictionary<ulong, Propose> DecreeState => _decreeState;

        public void Reset()
        {
            _lastVoteMessages.Clear();
            _voteMessages.Clear();
            _ongoingPropose.Clear();
            _lastTriedBallot.Clear();
            _decreeState.Clear();
        }
    }


    /// <summary>
    /// Event Recieved          Action
    /// LastVote                Return last vote if the new ballot no bigger than NextBallotNo recorded
    /// BeginNewBallot          If the new ballotNo is equal to NextBallotNo recorded,vote for the ballot
    ///                         and save the vote as last vote
    /// CommitDecree            Save the decree
    /// </summary>
    ///
    public class VoterRole
    {
        IPaxos _paxosPersisEvent;
        IPaxosNodeTalkChannel _nodeTalkChannel;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly VoterNote _note;

        public VoterRole(NodeInfo nodeInfo, PaxosCluster cluster, IPaxosNodeTalkChannel talkChannel, VoterNote paxoserNote)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeinfo");
            if (talkChannel == null) throw new ArgumentNullException("IPaxosNodeTalkChannel");
            if (paxoserNote == null) throw new ArgumentNullException("no note book");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _nodeTalkChannel = talkChannel;
            _note = paxoserNote;
        }

        public void DeliverNextBallotMessage(NextBallotMessage msg)
        {
            if (_note.NextBallotNo.ContainsKey(msg.DecreeNo) && _note.NextBallotNo[msg.DecreeNo] >= msg.BallotNo)
            {
                // do not response the ballotNo < current nextBallotNo
                var staleBallotMsg = new StaleBallotMessage()
                {
                    NextBallotNo = _note.NextBallotNo[msg.DecreeNo]
                };
                staleBallotMsg.TargetNode = msg.SourceNode;
                staleBallotMsg.BallotNo = msg.BallotNo;
                staleBallotMsg.DecreeNo = msg.DecreeNo;

                _nodeTalkChannel.SendMessage(staleBallotMsg);

                return;
            }

            VoteMessage lastVote = null;
            if (_note.VotedMessage.ContainsKey(msg.DecreeNo))
            {
                lastVote = _note.VotedMessage[msg.DecreeNo];
            }


            // send back the last vote infomation
            var lastVoteMsg = new LastVoteMessage();
            lastVoteMsg.TargetNode = msg.SourceNode;
            lastVoteMsg.BallotNo = msg.BallotNo;
            lastVoteMsg.DecreeNo = msg.DecreeNo;
            lastVoteMsg.VoteBallotNo = lastVote != null ? lastVote.BallotNo : 0;
            lastVoteMsg.VoteDecree = lastVote?.VoteDecree;

            if (!_note.NextBallotNo.ContainsKey(msg.DecreeNo))
            {
                _note.NextBallotNo.Add(msg.DecreeNo, msg.BallotNo);
            }
            else
            {
                _note.NextBallotNo[msg.DecreeNo] = msg.BallotNo;
            }

            _nodeTalkChannel.SendMessage(lastVoteMsg);
        }

        public void DeliverBeginBallotMessage(BeginBallotMessage msg)
        {
            if (!_note.NextBallotNo.ContainsKey(msg.DecreeNo))
            {
                return;
            }
            if (msg.BallotNo > _note.NextBallotNo[msg.DecreeNo])
            {
                return;
            }
            if (msg.BallotNo < _note.NextBallotNo[msg.DecreeNo])
            {
                var staleBallotMsg = new StaleBallotMessage()
                {
                    NextBallotNo = _note.NextBallotNo[msg.DecreeNo]
                };
                staleBallotMsg.TargetNode = msg.SourceNode;
                staleBallotMsg.BallotNo = msg.BallotNo;
                staleBallotMsg.DecreeNo = msg.DecreeNo;

                _nodeTalkChannel.SendMessage(staleBallotMsg);

                return;
            }

            // vote this ballot
            var voteMsg = new VoteMessage();
            voteMsg.TargetNode = msg.SourceNode;
            voteMsg.BallotNo = msg.BallotNo;
            voteMsg.DecreeNo = msg.DecreeNo;
            voteMsg.VoteDecree = msg.Decree;

            // save last vote
            if (_note.VotedMessage.ContainsKey(msg.DecreeNo))
            {
                _note.VotedMessage[msg.DecreeNo] = voteMsg;
            }
            else
            {
                _note.VotedMessage.Add(msg.DecreeNo, voteMsg);
            }

            // deliver the vote message
            _nodeTalkChannel.SendMessage(voteMsg);
        }

        public void DeliverSuccessMessage(SuccessMessage msg)
        {
            // save it to ledge
            _paxosPersisEvent?.UpdateSuccessfullDecree(msg.DecreeNo, msg.Decree);
        }
    }

    /// <summary>
    /// Three phase commit (3PC)
    /// 1. query exting vote
    /// 2. parepare commit
    /// 3. commit
    /// 
    ///     CurrentState            Event                   NextState               Action
    ///     Init                    Propose                 QueryLastVote           Query lastvote from cluster
    ///     QueryLastVote           ReceiveLastVote         BeginNewBallot(enough)  Send BeginNewBallot to quorum
    ///                                                     QueryLastVote(x)        Nothing
    ///     QueryLastVote           Timeout                 QueryLastVote           Query lastvote with new ballotNo
    ///     BeginNewBallot          ReceiveVote             BeginCommit(enough)     Send commit to all nodes
    ///                                                     ReceiveVote(x)          Nothing
    ///     BeginNewBallot          Timeout                 QueryLastVote           Send BeginNewBallot with a new ballotNo
    ///     BeginCommit             Response                Commited(enough)        Call subscriber
    ///                                                     BeginCommit(x)
    ///     BeginCommit             Timeout                 BeginCommit             Send commit to all nodes
    ///     
    /// </summary>
    public enum PropserState
    {
        Init,               // init
        QueryLastVote,      // query exiting vote
        BeginNewBallot,     // parepare commit
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
        private List<TaskCompletionSource<ProposeResult>> _subscribedCompletionSource = new List<TaskCompletionSource<ProposeResult>>();
        public PropserState State { get; set; }
        public List<TaskCompletionSource<ProposeResult>> CompletionEvents 
        { 
            get
            {
                return _subscribedCompletionSource;
            }
        }

    }

    public class ProposerRole
    {
        IPaxos _paxosPersisEvent;
        IPaxosNodeTalkChannel _nodeTalkChannel;
        private readonly NodeInfo _nodeInfo;
        private readonly PaxosCluster _cluster;

        private readonly ProposerNote _proposerNote;

        private List<Task> _tasksQueue = new List<Task>();
        private Task _messageHandlerTask;
        public ProposerRole(NodeInfo nodeInfo, PaxosCluster cluster, IPaxosNodeTalkChannel talkChannel, ProposerNote proposerNote)
        {
            if (cluster == null) throw new ArgumentNullException("cluster");
            if (nodeInfo == null) throw new ArgumentNullException("nodeinfo");
            if (talkChannel == null) throw new ArgumentNullException("IPaxosNodeTalkChannel");

            _nodeInfo = nodeInfo;
            _cluster = cluster;
            _nodeTalkChannel = talkChannel;
            _proposerNote = proposerNote;

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

        public bool HandleMessageAsync { get; set; }

        public Task<ProposeResult> BeginNewPropose(PaxosDecree decree, ulong nextDecreeNo)
        {
            var completionSource = new TaskCompletionSource<ProposeResult>();

            if (HandleMessageAsync)
            {
                Action action = () =>
                {
                    ProcessBeginNewBallotRequest(decree, nextDecreeNo, completionSource);
                };
                _tasksQueue.Add(new Task(action));
            }
            else
            {
                ProcessBeginNewBallotRequest(decree, nextDecreeNo, completionSource);
            }

            return completionSource.Task;
        }

        public void DeliverStaleBallotMessage(StaleBallotMessage msg)
        {
            if (HandleMessageAsync)
            {
                Action action = () =>
                {
                    ProcessStaleBallotMessage(msg);
                };
                _tasksQueue.Add(new Task(action));
            }
            else
            {
                ProcessStaleBallotMessage(msg);
            }
        }

        public void DeliverLastVoteMessage(LastVoteMessage msg)
        {
            if (HandleMessageAsync)
            {
                Action action = () =>
                {
                    ProcessLastVoteMessage(msg);
                };
                _tasksQueue.Add(new Task(action));
            }
            else
            {
                ProcessLastVoteMessage(msg);
            }
        }

        public void DeliverVoteMessage(VoteMessage msg)
        {
            if (HandleMessageAsync)
            {
                Action action = () =>
                {
                    ProcessVoteMessage(msg);
                };
                _tasksQueue.Add(new Task(action));
            }
            else
            {
                ProcessVoteMessage(msg);
            }
        }

        private void ProcessBeginNewBallotRequest(PaxosDecree decree, ulong nextDecreeNo, TaskCompletionSource<ProposeResult> completionSource)
        {
            ulong nextBallotNo = 1;
            if (nextDecreeNo == 0)
            {
                if (_proposerNote.LastTriedBallot.Count != 0)
                {
                    nextDecreeNo = _proposerNote.LastTriedBallot.Keys.Max() + 1;
                }
                else
                {
                    nextDecreeNo = 1;
                }
            }
            if (!_proposerNote.DecreeState.ContainsKey(nextDecreeNo))
            {
                _proposerNote.LastTriedBallot.Add(nextDecreeNo, nextBallotNo);
                _proposerNote.DecreeState.Add(nextDecreeNo, new Propose()
                {
                    State = PropserState.Init,
                });
                var completionEventList = _proposerNote.DecreeState[nextDecreeNo].CompletionEvents;

                completionEventList.Add(completionSource);
                _proposerNote.OngoingPropose.Add(nextDecreeNo, decree);

                _proposerNote.DecreeState[nextDecreeNo].State = PropserState.QueryLastVote;
                QueryLastVote(nextDecreeNo, nextBallotNo);
            }
            else
            {
                // already have a decree on it
                if (_proposerNote.DecreeState[nextDecreeNo].State == PropserState.BeginCommit)
                {
                    _proposerNote.DecreeState[nextDecreeNo].CompletionEvents.Add(completionSource);
                    // commit it
                    BeginCommit(nextDecreeNo);
                }
                else if (_proposerNote.DecreeState[nextDecreeNo].State == PropserState.Commited)
                {
                    var result = new ProposeResult()
                    {
                        DecreeNo = nextDecreeNo,
                        Decree = _proposerNote.OngoingPropose[nextDecreeNo]
                    };
                    completionSource.SetResult(result);
                    return;
                }
                else
                {
                    _proposerNote.LastTriedBallot.Add(nextDecreeNo, nextBallotNo);
                    _proposerNote.DecreeState.Add(nextDecreeNo, new Propose()
                    {
                        State = PropserState.Init
                    });
                    _proposerNote.OngoingPropose.Add(nextDecreeNo, decree);

                    _proposerNote.DecreeState[nextDecreeNo].CompletionEvents.Add(completionSource);
                    _proposerNote.DecreeState[nextDecreeNo].State = PropserState.QueryLastVote;
                    QueryLastVote(nextDecreeNo, nextBallotNo);
                }
            }
        }
        private void ProcessStaleBallotMessage(StaleBallotMessage msg)
        {
            /*
            if (!_decreeState.ContainsKey(msg.DecreeNo) ||
                _decreeState[msg.DecreeNo].State != PropserState.QueryLastVote)
            {
                return;
            }
            */

            if (!_proposerNote.LastTriedBallot.ContainsKey(msg.DecreeNo))
            {
                return;
            }
            if (_proposerNote.LastTriedBallot[msg.DecreeNo] != msg.BallotNo)
            {
                return;
            }

            if (_proposerNote.LastVoteMessages.ContainsKey(msg.DecreeNo))
            {
                _proposerNote.LastVoteMessages[msg.DecreeNo].Clear();
            }
            if (_proposerNote.VoteMessages.ContainsKey(msg.DecreeNo))
            {
                _proposerNote.VoteMessages[msg.DecreeNo].Clear();
            }

            // query last vote again
            ulong nextBallotNo = msg.NextBallotNo + 1;
            _proposerNote.LastTriedBallot[msg.DecreeNo] = nextBallotNo;

            _proposerNote.DecreeState[msg.DecreeNo].State = PropserState.QueryLastVote;
            QueryLastVote(msg.DecreeNo, nextBallotNo);
        }
        private void ProcessLastVoteMessage(LastVoteMessage msg)
        {
            if (!_proposerNote.DecreeState.ContainsKey(msg.DecreeNo) ||
                _proposerNote.DecreeState[msg.DecreeNo].State != PropserState.QueryLastVote)
            {
                return;
            }

            if (!_proposerNote.LastTriedBallot.ContainsKey(msg.DecreeNo))
            {
                return;
            }
            if (_proposerNote.LastTriedBallot[msg.DecreeNo] != msg.BallotNo)
            {
                return;
            }

            if (!_proposerNote.LastVoteMessages.ContainsKey(msg.DecreeNo))
            {
                _proposerNote.LastVoteMessages.Add(msg.DecreeNo, new List<LastVoteMessage>());
            }

            // TODO: check if msg come from existing node
            _proposerNote.LastVoteMessages[msg.DecreeNo].Add(msg);

            if (_proposerNote.LastVoteMessages[msg.DecreeNo].Count >= _cluster.Members.Count / 2 + 1)
            {
                // enough feedback got
                var maxVote = GetMaximumVote(msg.DecreeNo);
                if (maxVote != null)
                {
                    _proposerNote.OngoingPropose[msg.DecreeNo] = maxVote.VoteDecree;
                }

                _proposerNote.DecreeState[msg.DecreeNo].State = PropserState.BeginNewBallot;
                // begin new ballot
                BeginNewBallot(msg.DecreeNo, msg.BallotNo);
            }

        }

        private void ProcessVoteMessage(VoteMessage msg)
        {
            if (!_proposerNote.DecreeState.ContainsKey(msg.DecreeNo) ||
                _proposerNote.DecreeState[msg.DecreeNo].State != PropserState.BeginNewBallot)
            {
                return;
            }

            if (!_proposerNote.LastTriedBallot.ContainsKey(msg.DecreeNo))
            {
                // not recognized decree
                return;
            }
            if (_proposerNote.LastTriedBallot[msg.DecreeNo] != msg.BallotNo)
            {
                // not the vote ballot
                return;
            }

            if (!_proposerNote.VoteMessages.ContainsKey(msg.DecreeNo))
            {
                _proposerNote.VoteMessages.Add(msg.DecreeNo, new List<VoteMessage>());
            }

            // TODO: check if msg come from existing node
            _proposerNote.VoteMessages[msg.DecreeNo].Add(msg);

            if (_proposerNote.VoteMessages[msg.DecreeNo].Count >= _cluster.Members.Count / 2 + 1)
            {
                // enough feedback got, begin to commit
                _proposerNote.DecreeState[msg.DecreeNo].State = PropserState.BeginCommit;
                BeginCommit(msg.DecreeNo);
            }
        }

        private LastVoteMessage GetMaximumVote(UInt64 decreeNo)
        {
            if (!_proposerNote.LastVoteMessages.ContainsKey(decreeNo))
            {
                return null;
            }

            LastVoteMessage maxVoteMsg = null;
            foreach (var voteMsg in _proposerNote.LastVoteMessages[decreeNo])
            {
                if (maxVoteMsg == null || voteMsg.VoteBallotNo > maxVoteMsg.VoteBallotNo)
                {
                    maxVoteMsg = voteMsg;
                }
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
                beginBallotMessage.Decree = _proposerNote.OngoingPropose[decreeNo];
                _nodeTalkChannel.SendMessage(beginBallotMessage);
            }
        }

        private void BeginCommit(UInt64 decreeNo)
        {
            // write the decree to ledge

            _paxosPersisEvent?.UpdateSuccessfullDecree(decreeNo, _proposerNote.OngoingPropose[decreeNo]);

            foreach (var node in _cluster.Members)
            {
                if (node.Name == _nodeInfo.Name)
                {
                    continue;
                }
                var successMessage = new SuccessMessage();
                successMessage.TargetNode = node.Name;
                successMessage.DecreeNo = decreeNo;
                successMessage.BallotNo = _proposerNote.LastTriedBallot[decreeNo];
                successMessage.Decree = _proposerNote.OngoingPropose[decreeNo];
                _nodeTalkChannel.SendMessage(successMessage);
            }

            // TODO: confirm the commit succeeded on other nodes
            _proposerNote.DecreeState[decreeNo].State = PropserState.Commited; // commited
            var result = new ProposeResult()
            {
                DecreeNo = decreeNo,
                Decree = _proposerNote.OngoingPropose[decreeNo]
            };
            foreach (var completionSource in _proposerNote.DecreeState[decreeNo].CompletionEvents)
            {
                completionSource.SetResult(result);
            }
            _proposerNote.DecreeState[decreeNo].CompletionEvents.Clear();

        }
    }

    public class PaxosNode
    {
        IPaxosNodeTalkChannel _nodeTalkChannel;
        VoterRole _voterRole;
        ProposerRole _proposerRole;

        PaxosCluster _cluster;
        NodeInfo _nodeInfo;


        public PaxosNode(IPaxosNodeTalkChannel nodeTalkChannel, PaxosCluster cluster, NodeInfo nodeInfo)
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

            var voterNote = new VoterNote();
            _voterRole = new VoterRole(_nodeInfo, _cluster, _nodeTalkChannel, voterNote);
            var proposerNote = new ProposerNote();
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, _nodeTalkChannel, proposerNote);
        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decreee for this instance
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

        private Task<LastVoteMessage> GetLastVoteMessage()
        {
            var completionSource = new TaskCompletionSource<LastVoteMessage>();
            return completionSource.Task;
        }

    }

}
