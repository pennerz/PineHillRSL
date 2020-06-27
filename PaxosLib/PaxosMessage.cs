using PineRSL.Common;
using PineRSL.Paxos.Request;
using System;
using System.Collections.Generic;
using System.Text;

namespace PineRSL.Paxos.Message
{
    public enum PaxosMessageType
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
    public class PaxosMessage : ISer
    {
        public PaxosMessageType MessageType { get; set; }
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
            CheckpointedDecreNo = 0;
        }

        // all committed messages, whose decreee no > DecreeNo in this message
        public List<KeyValuePair<ulong, PaxosDecree>> CommittedDecrees { get; set; }

        public bool Commited { get; set; }
        public UInt64 CheckpointedDecreNo { get; set; }
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

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var isCommitedData = BitConverter.GetBytes(Commited);
            var checkpointedDecreeNoData = BitConverter.GetBytes(CheckpointedDecreNo);
            var votedBallotNoData = BitConverter.GetBytes(VoteBallotNo);
            var voteDecreeData = VoteDecree;
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(isCommitedData);
            dataList.Add(checkpointedDecreeNoData);
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
            CheckpointedDecreNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);

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
        public override byte[] Serialize()
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
        public override byte[] Serialize()
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

        public override byte[] Serialize()
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

    [Serializable()]
    public class CheckpointSummaryRequest : PaxosMessage, ISer
    {
        public CheckpointSummaryRequest()
        {
            MessageType = PaxosMessageType.CheckpointSummaryReq;
        }
    }

    [Serializable()]
    public class CheckpointSummaryResp : PaxosMessage, ISer
    {
        private String _checkpointFile;
        private UInt64 _checkpointDecreeNo;
        private UInt64 _checkpointDataLength;

        public CheckpointSummaryResp()
        {
            MessageType = PaxosMessageType.CheckpointSummaryResp;
            _checkpointFile = "";
            _checkpointDecreeNo = 0;
            _checkpointDataLength = 0;
        }

        public CheckpointSummaryResp(String chekcpointFile, UInt64 checkpointDecreeNo, UInt64 checkpointFileLength)
        {
            MessageType = PaxosMessageType.CheckpointSummaryResp;
            _checkpointFile = chekcpointFile;
            _checkpointDecreeNo = checkpointDecreeNo;
            _checkpointDataLength = checkpointFileLength;
        }

        public String CheckpointFile => _checkpointFile;
        public UInt64 CheckpointFileLength => _checkpointDataLength;
        public UInt64 CheckpointDecreeNo => _checkpointDecreeNo;

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();

            var checkpointFileData = Encoding.UTF8.GetBytes(_checkpointFile);
            var checkpointFileLengthData = BitConverter.GetBytes(_checkpointDataLength);
            var checkpointDecreeNoData = BitConverter.GetBytes(_checkpointDecreeNo);
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(checkpointFileData);
            dataList.Add(checkpointFileLengthData);
            dataList.Add(checkpointDecreeNoData);

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
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

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _checkpointFile = Encoding.UTF8.GetString(it.DataBuff, it.RecordOff, it.RecordSize);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _checkpointDataLength = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _checkpointDecreeNo = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
        }
    }


    [Serializable()]
    public class ReadCheckpointDataRequest : PaxosMessage, ISer
    {
        private String _checkpointFile;
        private UInt64 _offset;
        private UInt32 _length;

        public ReadCheckpointDataRequest()
        {
            MessageType = PaxosMessageType.CheckpointDataReq;
            _checkpointFile = "";
            _offset = 0;
            _length = 0;
        }

        public ReadCheckpointDataRequest(String chekcpointFile, UInt64 offset, UInt32 blockSize)
        {
            MessageType = PaxosMessageType.CheckpointDataReq;
            _checkpointFile = chekcpointFile;
            _offset = offset;
            _length = blockSize;
        }

        public string CheckpointFile => _checkpointFile;

        public UInt64 CheckpointFileOff => _offset;

        public UInt32 DataLength => _length;

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();

            var checkpointFileData = Encoding.UTF8.GetBytes(_checkpointFile);
            var offsetData = BitConverter.GetBytes(_offset);
            var lengthData = BitConverter.GetBytes(_length);
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(checkpointFileData);
            dataList.Add(offsetData);
            dataList.Add(lengthData);

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
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

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _checkpointFile = Encoding.UTF8.GetString(it.DataBuff, it.RecordOff, it.RecordSize);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _offset = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _length = BitConverter.ToUInt32(it.DataBuff, it.RecordOff);
        }
    }

    [Serializable()]
    public class ReadCheckpointDataResp : PaxosMessage, ISer
    {
        private String _checkpointFile;
        private UInt64 _offset;
        private UInt32 _length;
        private byte[] _data;

        public ReadCheckpointDataResp()
        {
            MessageType = PaxosMessageType.CheckpointDataResp;
            _checkpointFile = "";
            _offset = 0;
            _length = 0;
            _data = null;
        }

        public ReadCheckpointDataResp(String chekcpointFile, UInt64 offset, UInt32 blockSize, byte[] data)
        {
            MessageType = PaxosMessageType.CheckpointDataResp;
            _checkpointFile = chekcpointFile;
            _offset = offset;
            _length = blockSize;
            _data = data;
        }

        public byte[] Data => _data;

        public UInt32 DataLength => _length;

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();

            var checkpointFileData = Encoding.UTF8.GetBytes(_checkpointFile);
            var offsetData = BitConverter.GetBytes(_offset);
            var lengthData = BitConverter.GetBytes(_length);
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            dataList.Add(checkpointFileData);
            dataList.Add(offsetData);
            dataList.Add(lengthData);
            if (_data != null)
            {
                dataList.Add(_data);
            }

            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
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

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _checkpointFile = Encoding.UTF8.GetString(it.DataBuff, it.RecordOff, it.RecordSize);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _offset = BitConverter.ToUInt64(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _length = BitConverter.ToUInt32(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            _data = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, _data, 0, it.RecordSize);

        }
    }


    public class AggregatedPaxosMessage : PaxosMessage, ISer
    {
        private List<PaxosMessage> _paxosMessageList = new List<PaxosMessage>();
        public AggregatedPaxosMessage()
        {
            MessageType = PaxosMessageType.AGGREGATED;
        }

        public void AddPaxosMessage(PaxosMessage paxosMessage)
        {
            _paxosMessageList.Add(paxosMessage);
        }

        public List<PaxosMessage> PaxosMessages => _paxosMessageList;

        public override byte[] Serialize()
        {
            var serializeBuf = new SerializeBuffer();
            var baseSerializedData = base.Serialize();
            var dataList = new List<byte[]>();
            dataList.Add(baseSerializedData);
            foreach(var paxosMsg in _paxosMessageList)
            {
                dataList.Add(BitConverter.GetBytes((int)paxosMsg.MessageType));
                dataList.Add(paxosMsg.Serialize());
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
                var msgType = (PaxosMessageType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);

                it = it.Next();
                if (it.Equals(endIt))
                {
                    return;
                }
                var serializeData = new byte[it.RecordSize];
                Buffer.BlockCopy(it.DataBuff, it.RecordOff, serializeData, 0, it.RecordSize);
                var paxosMsg = MessageFactory.CreatePaxosMessage(msgType, serializeData);
                _paxosMessageList.Add(paxosMsg);
            }
        }

    }

    public class MessageFactory
    {
        public static PaxosMessage CreatePaxosMessage(PaxosMessageType messageType, byte[] serializedData)
        {
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
            }
            return null;
        }
    }

}
