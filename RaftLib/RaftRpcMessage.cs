﻿using PineHillRSL.Network;
using PineHillRSL.Rpc;
using PineHillRSL.Raft.Message;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PineHillRSL.Raft.Rpc
{
    public enum RaftRpcMessageType
    {
        EmptyResponse,
        QueryLastVote,
        LastVote,
        BeginNewBallot,
        Vote,
        Successfull,
        StaleBallot,
        Aggregated,
        CheckpointSummaryRequest,
        CheckpointSummaryResp,
        CheckpointDataRequest,
        CheckpointDataResp
    }

    /// <summary>
    /// Paxos protocol communication messages
    /// </summary>
    public class RaftRpcMessage
    {
        public RaftRpcMessageType MessageType { get; set; }
        public byte[] MessageContent { get; set; }
    }

    /// <summary>
    /// Paxos node's Rpc Request handler. It will call IMessageDeliver to
    /// deliver the paxos message to related roles. After that it reply
    /// the request.
    /// </summary>
    public class RaftMessageHandler : IRpcRequestHandler
    {
        private IMessageDeliver _messageDeliver;
        private IRpcRequestHandler _rpcRequestHandler;

        /// <summary>
        /// IMessageDeliver is interface which has method to deliver the paxos
        /// message to related roles
        /// </summary>
        /// <param name="paxosMessageDeliver"></param>
        /// <param name="rpcRequestHandler"></param>
        public RaftMessageHandler(IMessageDeliver paxosMessageDeliver, IRpcRequestHandler rpcRequestHandler)
        {
            _rpcRequestHandler = rpcRequestHandler;
            _messageDeliver = paxosMessageDeliver;
        }

        public async Task<RpcMessage> HandleRequest(RpcMessage request)
        {
            var paxosRpcMessage = RaftRpcMessageFactory.CreateRaftRpcMessage(request);
            switch (paxosRpcMessage.MessageType)
            {
                /*
                case PaxosRpcMessageType.QueryLastVote:
                case PaxosRpcMessageType.BeginNewBallot:
                case PaxosRpcMessageType.Vote:
                case PaxosRpcMessageType.Successfull:
                case PaxosRpcMessageType.LastVote:
                    if (_messageDeliver != null)
                    {
                        var paxosMessage = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMessage);
                        if (paxosMessage != null)
                        {
                            await _messageDeliver?.DeliverMessage(paxosMessage);
                        }
                        else
                        {
                            paxosMessage = null;
                        }
                    }
                    break;
                case PaxosRpcMessageType.Aggregated:
                    if (_messageDeliver != null)
                    {
                        var aggregatedMsg = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMessage) as AggregatedPaxosMessage;
                        if (aggregatedMsg != null)
                        {
                            var taskList = new List<Task>();
                            var taskWithRespList = new List<Task<PaxosMessage>>();
                            foreach (var paxosMessage in aggregatedMsg.PaxosMessages)
                            {
                                var task = _messageDeliver?.DeliverMessage(paxosMessage);
                                if (task != null)
                                {
                                    taskList.Add(task);
                                }
                            }
                            await Task.WhenAll(taskList);
                        }
                        else
                        {
                            aggregatedMsg = null;
                        }
                    }
                    break;
                case PaxosRpcMessageType.CheckpointSummaryRequest:
                case PaxosRpcMessageType.CheckpointDataRequest:
                    {
                        var paxosMessage = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMessage);
                        if (paxosMessage != null)
                        {
                            var resp = await _messageDeliver?.Request(paxosMessage);
                            var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(resp);
                            var rpcResp = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                            return rpcResp;
                        }
                        return null;
                    }
                default:
                    if (_rpcRequestHandler != null)
                    {
                        var resp = await _rpcRequestHandler.HandleRequest(request);
                        return resp;
                    }
                    break;*/
            }

            return new RpcMessage()
            {
                IsRequest = false,
                RequestId = request.RequestId
            };
        }

    }
}
