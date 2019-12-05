﻿using Paxos.Message;
using Paxos.Rpc;
using System;
using System.IO;
using System.Xml.Serialization;

namespace Paxos.Network
{

    public class PaxosRpcMessageFactory
    {
        public static PaxosRpcMessage CreatePaxosRpcMessage(RpcMessage rpcRequest)
        {
            var index = rpcRequest.RequestContent.IndexOf('_');
            var typestr = rpcRequest.RequestContent.Substring(0, index);
            var content = rpcRequest.RequestContent.Substring(index + 1);

            var paxosRpcMessage = new PaxosRpcMessage();
            paxosRpcMessage.MessageType = (PaxosRpcMessageType)Enum.Parse(typeof(PaxosRpcMessageType), typestr);
            paxosRpcMessage.MessageContent = content;

            return paxosRpcMessage;
        }

        public static RpcMessage CreateRpcRequest(PaxosRpcMessage paxosRpcMessage)
        {
            var rpcRequest = new RpcMessage();
            var msgTypeStr = paxosRpcMessage.MessageType.ToString();
            rpcRequest.RequestContent =  msgTypeStr + "_" + paxosRpcMessage.MessageContent;
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
            PaxosRpcMessage rpcMessage = new PaxosRpcMessage();
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

    public class Serializer<T>
        where T: class, new ()
    {
        public static string Serialize(T val)
        {
            var xmlSerializer = new XmlSerializer(typeof(T));
            var stream = new MemoryStream();
            xmlSerializer.Serialize(stream, val);
            var data = stream.GetBuffer();
            return Base64Encode(data);
        }

        public static T Deserialize(string str)
        {
            var data = Base64Decode(str);

            var stream = new MemoryStream(data);
            var xmlSerializer = new XmlSerializer(typeof(T));
            return xmlSerializer.Deserialize(stream) as T;
        }

        private static string Base64Encode(byte[] data)
        {
            return System.Convert.ToBase64String(data);
        }
        private static byte[] Base64Decode(string base64EncodedData)
        {
            return System.Convert.FromBase64String(base64EncodedData);
        }
    }

}
