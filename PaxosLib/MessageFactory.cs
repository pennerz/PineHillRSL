using Paxos.Message;
using Paxos.Rpc;
using Paxos.Common;
using System;
using System.Collections.Generic;

namespace Paxos.Network
{

    public class PaxosRpcMessageFactory
    {
        public static PaxosRpcMessage CreatePaxosRpcMessage(RpcMessage rpcRequest)
        {
            var paxosRpcMessage = new PaxosRpcMessage();
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(rpcRequest.RequestContent);
            var itEnd = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(itEnd))
            {
                return null;
            }
            paxosRpcMessage.MessageType = (PaxosRpcMessageType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(itEnd))
            {
                return paxosRpcMessage;
            }
            var messageContent = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, messageContent, 0, it.RecordSize);
            paxosRpcMessage.MessageContent = messageContent;

            /*
            var index = rpcRequest.RequestContent.IndexOf('_');
            var typestr = rpcRequest.RequestContent.Substring(0, index);
            var content = rpcRequest.RequestContent.Substring(index + 1);

            paxosRpcMessage.MessageType = (PaxosRpcMessageType)Enum.Parse(typeof(PaxosRpcMessageType), typestr);
            paxosRpcMessage.MessageContent = content;
            */

            return paxosRpcMessage;
        }

        public static RpcMessage CreateRpcRequest(PaxosRpcMessage paxosRpcMessage)
        {
            var rpcRequest = new RpcMessage();
            var serializeBuf = new SerializeBuffer();
            var messageTypeData = BitConverter.GetBytes((int)paxosRpcMessage.MessageType);
            var messageContentData = paxosRpcMessage.MessageContent;
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(messageContentData);
            serializeBuf.AppendBlocks(dataList);
            rpcRequest.RequestContent = serializeBuf.DataBuf;
            /*
            var msgTypeStr = paxosRpcMessage.MessageType.ToString();
            rpcRequest.RequestContent =  msgTypeStr + "_" + paxosRpcMessage.MessageContent;*/
            return rpcRequest;
        }
    }
    public class PaxosMessageFactory
    {
        public static PaxosMessage CreatePaxosMessage(PaxosRpcMessage rpcMessage)
        {
            switch (rpcMessage.MessageType)
            {
                case PaxosRpcMessageType.QueryLastVote:
                    {
                        return Serializer<NextBallotMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case PaxosRpcMessageType.LastVote:
                    {
                        return Serializer<LastVoteMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case PaxosRpcMessageType.BeginNewBallot:
                    {
                        return Serializer<BeginBallotMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case PaxosRpcMessageType.Vote:
                    {
                        return Serializer<VoteMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case PaxosRpcMessageType.Successfull:
                    {
                        return Serializer<SuccessMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case PaxosRpcMessageType.StaleBallot:
                    {
                        return Serializer<StaleBallotMessage>.Deserialize(rpcMessage.MessageContent);
                    }
            }
            return null;
        }

        public static PaxosRpcMessage CreatePaxosRpcMessage(PaxosMessage paxosMessage)
        {
            var begin = DateTime.Now;
            PaxosRpcMessage rpcMessage = new PaxosRpcMessage();
            var allocateObjCostTime = DateTime.Now - begin;
            if (allocateObjCostTime.TotalMilliseconds > 500)
            {
                //Console.WriteLine("too slow");
            }
            switch (paxosMessage.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.QueryLastVote;
                        rpcMessage.MessageContent = Serializer<NextBallotMessage>.Serialize(paxosMessage as NextBallotMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.LASTVOTE:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.LastVote;
                        rpcMessage.MessageContent = Serializer<LastVoteMessage>.Serialize(paxosMessage as LastVoteMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.BEGINBALLOT:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.BeginNewBallot;
                        rpcMessage.MessageContent = Serializer<BeginBallotMessage>.Serialize(paxosMessage as BeginBallotMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.VOTE:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.Vote;
                        rpcMessage.MessageContent = Serializer<VoteMessage>.Serialize(paxosMessage as VoteMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.SUCCESS:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.Successfull;
                        rpcMessage.MessageContent = Serializer<SuccessMessage>.Serialize(paxosMessage as SuccessMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.STALEBALLOT:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.StaleBallot;
                        rpcMessage.MessageContent = Serializer<StaleBallotMessage>.Serialize(paxosMessage as StaleBallotMessage);
                        return rpcMessage;
                    }
            }

            return null;
        }
    }


}
