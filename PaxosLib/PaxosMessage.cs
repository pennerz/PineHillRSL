using System;
using System.Collections.Generic;
using System.Text;

namespace Paxos.Message
{
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
        public PaxosMessageType MessageType { get; set; }
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

        public bool Commited { get; set; }
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
}
