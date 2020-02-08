using Paxos.Common;
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

        public byte[] Serialize()
        {
            var messageTypeData = BitConverter.GetBytes((int)MessageType);
            var sourceNodeData = Encoding.UTF8.GetBytes(SourceNode);
            var targetNodeData = Encoding.UTF8.GetBytes(TargetNode);
            var decreeNoData = BitConverter.GetBytes(DecreeNo);
            var ballotNoData = BitConverter.GetBytes(BallotNo);
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(sourceNodeData);
            dataList.Add(targetNodeData);
            dataList.Add(decreeNoData);
            dataList.Add(ballotNoData);
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.AppendBlocks(dataList);
            return serializeBuffer.DataBuf;

            //return "MessageType:" + MessageType.ToString() + "_" +
            //"SourceNode:" + SourceNode + "_" +
            //"TargetNode:" + TargetNode + "_" +
            //"DecreeNo:" + DecreeNo.ToString() + "_" +
            //"BallotNo:" + BallotNo.ToString() + "_";
        }

        public void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            int messageType = BitConverter.ToInt32(it.DataBuff, it.RecordOff);
            MessageType = (PaxosMessageType)messageType;

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            SourceNode = Encoding.UTF8.GetString(it.DataBuff, it.RecordOff, it.RecordSize);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            TargetNode = Encoding.UTF8.GetString(it.DataBuff, it.RecordOff, it.RecordSize);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            DecreeNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            BallotNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            /*
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
                    break;
                }
            }*/
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
        public UInt64 VoteBallotNo { get; set; }
        public byte[] VoteDecree { get; set; }
        public string VoteDecreeContent
        {
            get
            {
                return VoteDecree != null ? Encoding.UTF8.GetString(VoteDecree) : null;
            }
            set
            {
                VoteDecree = value != null ? Encoding.UTF8.GetBytes(value) : null;
            }
        }

        public new byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var isCommitedData = BitConverter.GetBytes(Commited);
            var votedBallotNoData = BitConverter.GetBytes(VoteBallotNo);
            var voteDecreeData = VoteDecree;
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(isCommitedData);
            dataList.Add(votedBallotNoData);
            if (voteDecreeData != null)
            {
                dataList.Add(voteDecreeData);
            }

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
            /*
            return base.Serialize() + "Commited:" + Commited.ToString() + "_" +
            "VoteBallotNo:" + VoteBallotNo.ToString() + "_" +
            "VoteDecree:" + "_" + VoteDecree;*/
        }

        public new void DeSerialize(byte[] data)
        {
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var endIt = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(endIt))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            Commited = BitConverter.ToBoolean(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            VoteBallotNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            VoteDecree = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, VoteDecree, 0, it.RecordSize);

            /*
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
                    VoteDecree = str;
                    break;
                }
            }
        }*/
        }
    }

    [Serializable()]
    public class BeginBallotMessage : PaxosMessage, ISer
    {
        public BeginBallotMessage()
        {
            MessageType = PaxosMessageType.BEGINBALLOT;
        }
        public byte[] Decree { get; set; }
        public string DecreeContent
        {
            get
            {
                return Encoding.UTF8.GetString(Decree);
            }
            set
            {
                Decree = Encoding.UTF8.GetBytes(value);
            }
        }
        public new byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var decreeData = Decree;
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(decreeData);

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;

            //return base.Serialize() + "Decree:" + "_" + Decree;
        }

        public new void DeSerialize(byte[] data)
        {
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var endIt = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(endIt))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            Decree = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, Decree, 0, it.RecordSize);

            /*
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
                    Decree = str;
                    break;
                }
            }*/
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
        public byte[] Decree { get; set; }
        public new byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var decreeData = Decree;
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(decreeData);

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;

            //return base.Serialize() + "Decree:" + "_" + Decree;
        }

        public new void DeSerialize(byte[] data)
        {
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var endIt = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(endIt))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            Decree = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, Decree, 0, it.RecordSize);


            /*
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
                    Decree = str;
                    break;
                }
            }*/
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
        public new byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var nextBallotNoData = BitConverter.GetBytes(NextBallotNo);
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(nextBallotNoData);

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
            //return base.Serialize() + "NextBallotNo:" + NextBallotNo.ToString() + "_";
        }

        public new void DeSerialize(byte[] data)
        {
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var endIt = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(endIt))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            NextBallotNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            /*
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
            }*/
        }
    }
}
