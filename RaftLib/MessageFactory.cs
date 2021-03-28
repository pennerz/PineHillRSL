using PineHillRSL.Raft.Message;
using PineHillRSL.Common;
using PineHillRSL.Rpc;
using System;
using System.Collections.Generic;

namespace PineHillRSL.Raft.Rpc
{

    public class RaftRpcMessageFactory
    {
        public static RaftRpcMessage CreateRaftRpcMessage(RpcMessage rpcRequest)
        {
            var paxosRpcMessage = new RaftRpcMessage();
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(rpcRequest.RequestContent);
            var itEnd = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(itEnd))
            {
                return null;
            }
            paxosRpcMessage.MessageType = (RaftRpcMessageType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);

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

        public static RpcMessage CreateRpcRequest(RaftRpcMessage paxosRpcMessage)
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
    public class RaftMessageFactory
    {
        public static RaftMessage CreateRaftMessage(RaftRpcMessage rpcMessage)
        {
            switch (rpcMessage.MessageType)
            {
                case RaftRpcMessageType.AppendEntityReq:
                    {
                        return Serializer<AppendEntityReqMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case RaftRpcMessageType.AppendEntityResp:
                    {
                        return Serializer<AppendEntityRespMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case RaftRpcMessageType.VoteLeaderReq:
                    {
                        return Serializer<VoteReqMessage>.Deserialize(rpcMessage.MessageContent);
                    }
                case RaftRpcMessageType.VoteLeaderResp:
                    {
                        return Serializer<VoteRespMessage>.Deserialize(rpcMessage.MessageContent);
                    }

                    /*
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
                    case PaxosRpcMessageType.Aggregated:
                        {
                            return Serializer<AggregatedPaxosMessage>.Deserialize(rpcMessage.MessageContent);
                        }
                    case PaxosRpcMessageType.CheckpointSummaryRequest:
                        {
                            return Serializer<CheckpointSummaryRequest>.Deserialize(rpcMessage.MessageContent);
                        }
                    case PaxosRpcMessageType.CheckpointSummaryResp:
                        {
                            return Serializer<CheckpointSummaryResp>.Deserialize(rpcMessage.MessageContent);
                        }
                    case PaxosRpcMessageType.CheckpointDataRequest:
                        {
                            return Serializer<ReadCheckpointDataRequest>.Deserialize(rpcMessage.MessageContent);
                        }
                    case PaxosRpcMessageType.CheckpointDataResp:
                        {
                            return Serializer<ReadCheckpointDataResp>.Deserialize(rpcMessage.MessageContent);
                        }*/
            }
            return null;
        }

        public static RaftRpcMessage CreateRaftRpcMessage(RaftMessage raftMessage)
        {
            var begin = DateTime.Now;
            RaftRpcMessage rpcMessage = new RaftRpcMessage();
            /*
            var allocateObjCostTime = DateTime.Now - begin;
            if (allocateObjCostTime.TotalMilliseconds > 500)
            {
                Console.WriteLine($"new paxos rpc message too slow, {allocateObjCostTime.TotalMilliseconds}ms");
            }*/

            switch (raftMessage.MessageType)
            {
                case RaftMessageType.AppendEntityReq:
                    {
                        rpcMessage.MessageType = RaftRpcMessageType.AppendEntityReq;
                        rpcMessage.MessageContent = Serializer<AppendEntityReqMessage>.Serialize(raftMessage as AppendEntityReqMessage);
                        return rpcMessage;
                    }
                case RaftMessageType.AppendEntityResp:
                    {
                        rpcMessage.MessageType = RaftRpcMessageType.AppendEntityResp;
                        rpcMessage.MessageContent = Serializer<AppendEntityRespMessage>.Serialize(raftMessage as AppendEntityRespMessage);
                        return rpcMessage;
                    }
                case RaftMessageType.VoteReq:
                    {
                        rpcMessage.MessageType = RaftRpcMessageType.VoteLeaderReq;
                        rpcMessage.MessageContent = Serializer<VoteReqMessage>.Serialize(raftMessage as VoteReqMessage);
                        return rpcMessage;
                    }
                case RaftMessageType.VoteResp:
                    {
                        rpcMessage.MessageType = RaftRpcMessageType.VoteLeaderResp;
                        rpcMessage.MessageContent = Serializer<VoteRespMessage>.Serialize(raftMessage as VoteRespMessage);
                        return rpcMessage;
                    }
                    /*
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
                case PaxosMessageType.AGGREGATED:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.Aggregated;
                        rpcMessage.MessageContent = Serializer<AggregatedPaxosMessage>.Serialize(paxosMessage as AggregatedPaxosMessage);
                        return rpcMessage;
                    }
                case PaxosMessageType.CheckpointSummaryReq:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.CheckpointSummaryRequest;
                        rpcMessage.MessageContent = Serializer<CheckpointSummaryRequest>.Serialize(paxosMessage as CheckpointSummaryRequest);
                        return rpcMessage;

                    }
                case PaxosMessageType.CheckpointSummaryResp:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.CheckpointSummaryResp;
                        rpcMessage.MessageContent = Serializer<CheckpointSummaryResp>.Serialize(paxosMessage as CheckpointSummaryResp);
                        return rpcMessage;
                    }
                case PaxosMessageType.CheckpointDataReq:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.CheckpointDataRequest;
                        rpcMessage.MessageContent = Serializer<ReadCheckpointDataRequest>.Serialize(paxosMessage as ReadCheckpointDataRequest);
                        return rpcMessage;
                    }
                case PaxosMessageType.CheckpointDataResp:
                    {
                        rpcMessage.MessageType = PaxosRpcMessageType.CheckpointDataResp;
                        rpcMessage.MessageContent = Serializer<ReadCheckpointDataResp>.Serialize(paxosMessage as ReadCheckpointDataResp);
                        return rpcMessage;

                    }*/
            }

            return null;
        }
    }


}
