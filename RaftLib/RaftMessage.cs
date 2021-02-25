using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace PineHillRSL.Raft.Message
{
    public enum RaftMessageType
    {
        NEXTBALLOT,
        LASTVOTE,
        BEGINBALLOT,
        VOTE,
        SUCCESS,
        STALEBALLOT,
        AGGREGATED,
        CheckpointSummaryReq,
        CheckpointSummaryResp,
        CheckpointDataReq,
        CheckpointDataResp,
    }

    /// <summary>
    /// Paxos protocol message
    /// </summary>
    [Serializable()]
    public class RaftMessage : ISer
    {
        public RaftMessageType MessageType { get; set; }
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        public UInt64 DecreeNo { get; set; }
        public UInt64 BallotNo { get; set; }

        public virtual byte[] Serialize()
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

        public virtual void DeSerialize(byte[] data)
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
            MessageType = (RaftMessageType)messageType;

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

/*
    [Serializable()]
    public class NextBallotMessage : PaxosMessage, ISer
    {
        public NextBallotMessage()
        {
            MessageType = PaxosMessageType.NEXTBALLOT;
        }
    }
*/

    public class MessageFactory
    {
        public static RaftMessage CreateRaftMessage(RaftMessageType messageType, byte[] serializedData)
        {
        /*
            switch (messageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    {
                        return Serializer<NextBallotMessage>.Deserialize(serializedData);
                    }
                case PaxosMessageType.LASTVOTE:
                    {
                        return Serializer<LastVoteMessage>.Deserialize(serializedData);
                    }
                case PaxosMessageType.BEGINBALLOT:
                    {
                        return Serializer<BeginBallotMessage>.Deserialize(serializedData);
                    }
                case PaxosMessageType.VOTE:
                    {
                        return Serializer<VoteMessage>.Deserialize(serializedData);
                    }
                case PaxosMessageType.SUCCESS:
                    {
                        return Serializer<SuccessMessage>.Deserialize(serializedData);
                    }
                case PaxosMessageType.STALEBALLOT:
                    {
                        return Serializer<StaleBallotMessage>.Deserialize(serializedData);
                    }
            }*/
            return null;
        }
    }

}
