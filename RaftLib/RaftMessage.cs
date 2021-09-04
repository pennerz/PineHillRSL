using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace PineHillRSL.Raft.Message
{
    public enum RaftMessageType
    {
        VoteReq,
        VoteResp,
        AppendEntityReq,
        AppendEntityResp,
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
        public RaftMessage()
        { }

        public RaftMessage(RaftMessage rhs)
        {
            SourceNode = rhs.SourceNode;
            TargetNode = rhs.TargetNode;
            Term = rhs.Term;
            LogIndex = rhs.LogIndex;
        }

        public RaftMessageType MessageType { get; set; }
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        //public UInt64 DecreeNo { get; set; }
        //public UInt64 BallotNo { get; set; }

        // for raft
        public UInt64 Term { get; set; } = 0;
        public UInt64 LogIndex { get; set; } = 0;


        public virtual byte[] Serialize()
        {
            var messageTypeData = BitConverter.GetBytes((Int32)MessageType);
            var sourceNodeData = Encoding.UTF8.GetBytes(SourceNode);
            var targetNodeData = Encoding.UTF8.GetBytes(TargetNode);
            var termData = BitConverter.GetBytes(Term);
            var logIndexData = BitConverter.GetBytes(LogIndex);
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(sourceNodeData);
            dataList.Add(targetNodeData);
            dataList.Add(termData);
            dataList.Add(logIndexData);
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
            Term = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            LogIndex = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

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
    public class VoteReqMessage : RaftMessage
    {
        public VoteReqMessage()
        {
            MessageType = RaftMessageType.VoteReq;
        }

        public VoteReqMessage(VoteReqMessage rhs)
            : base(rhs)
        {
            MessageType = RaftMessageType.VoteReq;
            LastLogEntryTerm = rhs.LastLogEntryTerm;
            LastLogEntryIndex = rhs.LastLogEntryIndex;
        }

        public UInt64 LastLogEntryTerm { get; set; } = 0;
        public UInt64 LastLogEntryIndex { get; set; } = 0;

        public VoteReqMessage DeepCopy()
        {
            var copy = new VoteReqMessage(this);
            return copy;
        }

        public override byte[] Serialize()
        {
            var lastLogEntryTermData = BitConverter.GetBytes(LastLogEntryTerm);
            var lastLogEntryIndexData = BitConverter.GetBytes(LastLogEntryIndex);
            var baseData = base.Serialize();

            var dataList = new List<byte[]>();
            dataList.Add(lastLogEntryTermData);
            dataList.Add(lastLogEntryIndexData);
            dataList.Add(baseData);

            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.AppendBlocks(dataList);
            return serializeBuffer.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            LastLogEntryTerm = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            LastLogEntryIndex = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }

            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);
        }
    }

    [Serializable()]
    public class VoteRespMessage : RaftMessage
    {
        public VoteRespMessage()
        {
            MessageType = RaftMessageType.VoteResp;
        }

        public bool Succeed { get; set; }

        public override byte[] Serialize()
        {
            var dataList = new List<byte[]>();
            dataList.Add(base.Serialize());
            var isSucceededBuf = BitConverter.GetBytes(Succeed);
            dataList.Add(isSucceededBuf);

            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.AppendBlocks(dataList);
            return serializeBuffer.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            Succeed = BitConverter.ToBoolean(it.DataBuff, it.RecordOff);
        }
    }

    [Serializable()]
    public class AppendEntityReqMessage : RaftMessage
    {
        public AppendEntityReqMessage()
        {
            MessageType = RaftMessageType.AppendEntityReq;
        }

        public AppendEntityReqMessage(AppendEntityReqMessage rhs)
            : base(rhs)
        {
            MessageType = RaftMessageType.AppendEntityReq;
            CommittedLogIndex = rhs.CommittedLogIndex;
            PreviousLogIndex = rhs.PreviousLogIndex;
            PreviousLogTerm = rhs.PreviousLogTerm;
            Data = rhs.Data;
        }

        public UInt64 CommittedLogIndex { get; set; } = 0;

        public UInt64 PreviousLogTerm { get; set; } = 0;
        public UInt64 PreviousLogIndex { get; set; } = 0;

        public byte[] Data { get; set; }

        public override byte[] Serialize()
        {
            var dataList = new List<byte[]>();
            dataList.Add(base.Serialize());
            var membBuf = BitConverter.GetBytes(CommittedLogIndex);
            dataList.Add(membBuf);
            membBuf = BitConverter.GetBytes(PreviousLogTerm);
            dataList.Add(membBuf);
            membBuf = BitConverter.GetBytes(PreviousLogIndex);
            dataList.Add(membBuf);

            dataList.Add(Data);

            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.AppendBlocks(dataList);
            return serializeBuffer.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            CommittedLogIndex = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            PreviousLogTerm = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            PreviousLogIndex = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);


            it = it.Next();
            if (it.Equals(itEnd))
            {
                Data = null;
                return;
            }

            Data = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, Data, 0, it.RecordSize);

        }

        public AppendEntityReqMessage DeepCopy()
        {
            return new AppendEntityReqMessage(this);
        }
    }

    [Serializable()]
    public class AppendEntityRespMessage : RaftMessage
    {
        public enum AppendResult
        {
            Succeed,
            StaleTerm,
            AlreadyCommitted,
            HoleExit,
            Fail
        }
        public AppendEntityRespMessage()
        {
            MessageType = RaftMessageType.AppendEntityResp;
        }

        public AppendResult Result { get; set; } = AppendResult.Fail;

        public Int64 PreviousTerm { get; set; }
        public Int64 PreviousLogIndex { get; set; }

        public override byte[] Serialize()
        {
            var dataList = new List<byte[]>();
            dataList.Add(base.Serialize());
            var isResultBuf = BitConverter.GetBytes((UInt32)Result);
            dataList.Add(isResultBuf);

            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.AppendBlocks(dataList);
            return serializeBuffer.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuffer = new SerializeBuffer();
            serializeBuffer.ConcatenateBuff(data);

            var itEnd = serializeBuffer.End();
            var it = serializeBuffer.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            var baseSerializeData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, baseSerializeData, 0, it.RecordSize);
            base.DeSerialize(baseSerializeData);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            Result = (AppendResult)BitConverter.ToUInt32(it.DataBuff, it.RecordOff);
        }

    }

    public class AggregatedRaftMessage : RaftMessage, ISer
    {
        private List<RaftMessage> _raftMessageList = new List<RaftMessage>();
        public AggregatedRaftMessage()
        {
            MessageType = RaftMessageType.AGGREGATED;
        }

        public void AddRaftMessage(RaftMessage raftMessage)
        {
            _raftMessageList.Add(raftMessage);
        }

        public List<RaftMessage> RaftMessages => _raftMessageList;

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            foreach (var raftMsg in _raftMessageList)
            {
                dataList.Add(BitConverter.GetBytes((int)raftMsg.MessageType));
                dataList.Add(raftMsg.Serialize());
            }
            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
            //return base.Serialize() + "NextBallotNo:" + NextBallotNo.ToString() + "_";
        }

        public override void DeSerialize(byte[] data)
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

            for (it = it.Next(); !it.Equals(endIt); it = it.Next())
            {
                var msgType = (RaftMessageType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);

                it = it.Next();
                if (it.Equals(endIt))
                {
                    return;
                }
                var serializeData = new byte[it.RecordSize];
                Buffer.BlockCopy(it.DataBuff, it.RecordOff, serializeData, 0, it.RecordSize);
                var raftMsg = MessageFactory.CreateRaftMessage(msgType, serializeData);
                _raftMessageList.Add(raftMsg);
            }
        }

    }



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
