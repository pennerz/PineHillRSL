using Paxos.Common;
using Paxos.Request;
using System;
using System.Collections.Generic;

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

    /// <summary>
    /// Paxos protocol message
    /// </summary>
    [Serializable()]
    public class PaxosMessage : ISer
    {
        public PaxosMessageType MessageType { get; set; }
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        public UInt64 DecreeNo { get; set; }
        public UInt64 BallotNo { get; set; }

        public string Serialize()
        {
            return "MessageType:" + MessageType.ToString() + "_" +
            "SourceNode:" + SourceNode + "_" +
            "TargetNode:" + TargetNode + "_" +
            "DecreeNo:" + DecreeNo.ToString() + "_" +
            "BallotNo:" + BallotNo.ToString() + "_";
        }

        public void DeSerialize(string str)
        {
            while (str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("MessageType"))
                {
                    PaxosMessageType result;
                    PaxosMessageType.TryParse(value, out result);
                    MessageType = result;
                }
                else if (name.Equals("SourceNode"))
                {
                    SourceNode = value;
                }
                else if (name.Equals("TargetNode"))
                {
                    TargetNode = value;
                }
                else if (name.Equals("DecreeNo"))
                {
                    UInt64 result = 0;
                    UInt64.TryParse(value, out result);
                    DecreeNo = result;
                }
                else if (name.Equals("BallotNo"))
                {
                    UInt64 result = 0;
                    UInt64.TryParse(value, out result);
                    BallotNo = result;
                }
            }
        }

    }

    [Serializable()]
    public class NextBallotMessage : PaxosMessage, ISer
    {
        public NextBallotMessage()
        {
            MessageType = PaxosMessageType.NEXTBALLOT;
        }
    }

    [Serializable()]
    public class LastVoteMessage : PaxosMessage, ISer
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
        public string VoteDecree { get; set; }

        public new string Serialize()
        {
            return base.Serialize() + "Commited:" + Commited.ToString() + "_" +
            "VoteBallotNo:" + VoteBallotNo.ToString() + "_" +
            "VoteDecree:" + VoteDecree + "_";
        }

        public new void DeSerialize(string str)
        {
            base.DeSerialize(str);
            while(str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("Commited"))
                {
                    bool result = false;
                    bool.TryParse(value, out result);
                    Commited = result;
                }
                else if (name.Equals("VoteBallotNo"))
                {
                    ulong result = 0;
                    ulong.TryParse(value, out result);
                    VoteBallotNo = result;
                }
                else if (name.Equals("VoteDecree"))
                {
                    VoteDecree = value;
                }
            }
        }
    }

    [Serializable()]
    public class BeginBallotMessage : PaxosMessage, ISer
    {
        public BeginBallotMessage()
        {
            MessageType = PaxosMessageType.BEGINBALLOT;
        }
        public string Decree { get; set; }
        public new string Serialize()
        {
            return base.Serialize() + "Decree:" + Decree + "_";
        }

        public new void DeSerialize(string str)
        {
            base.DeSerialize(str);
            while (str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("Decree"))
                {
                    Decree = value;
                }
            }
        }
    }

    [Serializable()]
    public class VoteMessage : PaxosMessage, ISer
    {
        public VoteMessage()
        {
            MessageType = PaxosMessageType.VOTE;
        }
    }

    [Serializable()]
    public class SuccessMessage : PaxosMessage, ISer
    {
        public SuccessMessage()
        {
            MessageType = PaxosMessageType.SUCCESS;
        }
        public string Decree { get; set; }
        public new string Serialize()
        {
            return base.Serialize() + "Decree:" + Decree + "_";
        }

        public new void DeSerialize(string str)
        {
            base.DeSerialize(str);
            while (str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("Decree"))
                {
                    Decree = value;
                }
            }
        }
    }

    [Serializable()]
    public class StaleBallotMessage : PaxosMessage, ISer
    {
        public StaleBallotMessage()
        {
            MessageType = PaxosMessageType.STALEBALLOT;
        }

        public ulong NextBallotNo { get; set; }
        public new string Serialize()
        {
            return base.Serialize() + "NextBallotNo:" + NextBallotNo.ToString() + "_";
        }

        public new void DeSerialize(string str)
        {
            base.DeSerialize(str);
            while (str != null)
            {
                var index = str.IndexOf('_');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("NextBallotNo"))
                {
                    ulong result = 0;
                    ulong.TryParse(value, out result);
                    NextBallotNo = result ;
                }
            }
        }
    }
}
