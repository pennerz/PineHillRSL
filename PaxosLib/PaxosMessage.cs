using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Text;

namespace Paxos.Message
{
    public enum PaxosMessageType
    {
        NEXTBALLOT,
        LASTVOTE,
        BEGINBALLOT,
        VOTE,
        SUCCESS,
        STALEBALLOT
    }

    [Serializable()]
    public class PaxosMessage
    {
        public PaxosMessageType MessageType { get; set; }
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        public UInt64 DecreeNo { get; set; }
        public UInt64 BallotNo { get; set; }
    }

    [Serializable()]
    public class NextBallotMessage : PaxosMessage
    {
        public NextBallotMessage()
        {
            MessageType = PaxosMessageType.NEXTBALLOT;
        }
    }

    [Serializable()]
    public class LastVoteMessage : PaxosMessage
    {

        public LastVoteMessage()
        {
            MessageType = PaxosMessageType.LASTVOTE;
            CommittedDecrees = new List<KeyValuePair<ulong, PaxosDecree>>();
        }

        // all committed messages, whose decreee no > DecreeNo in this message
        public List<KeyValuePair<ulong, PaxosDecree>> CommittedDecrees { get; set; }

        public bool Commited { get; set; }
        public ulong VoteBallotNo { get; set; }
        public PaxosDecree VoteDecree { get; set; }
    }

    [Serializable()]
    public class BeginBallotMessage : PaxosMessage
    {
        public BeginBallotMessage()
        {
            MessageType = PaxosMessageType.BEGINBALLOT;
        }
        public PaxosDecree Decree { get; set; }
    }

    [Serializable()]
    public class VoteMessage : PaxosMessage
    {
        public VoteMessage()
        {
            MessageType = PaxosMessageType.VOTE;
        }
        public PaxosDecree VoteDecree { get; set; }
    }

    [Serializable()]
    public class SuccessMessage : PaxosMessage
    {
        public SuccessMessage()
        {
            MessageType = PaxosMessageType.SUCCESS;
        }
        public PaxosDecree Decree { get; set; }
    }

    [Serializable()]
    public class StaleBallotMessage : PaxosMessage
    {
        public StaleBallotMessage()
        {
            MessageType = PaxosMessageType.STALEBALLOT;
        }

        public ulong NextBallotNo { get; set; }
    }
}
