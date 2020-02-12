using Paxos.Network;
using Paxos.Message;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Paxos.Rpc
{
    public enum PaxosRpcMessageType
    {
        EmptyResponse,
        QueryLastVote,
        LastVote,
        BeginNewBallot,
        Vote,
        Successfull,
        StaleBallot,
        Aggregated,
    }

    /// <summary>
    /// Paxos protocol communication messages
    /// </summary>
    public class PaxosRpcMessage
    {
        public PaxosRpcMessageType MessageType { get; set; }
        public byte[] MessageContent { get; set; }
    }

    /// <summary>
    /// Paxos node's Rpc Request handler. It will call IMessageDeliver to
    /// deliver the paxos message to related roles. After that it reply
    /// the request.
    /// </summary>
    public class PaxosMessageHandler : IRpcRequestHandler
    {
        private IMessageDeliver _messageDeliver;
        private IRpcRequestHandler _rpcRequestHandler;

        /// <summary>
        /// IMessageDeliver is interface which has method to deliver the paxos
        /// message to related roles
        /// </summary>
        /// <param name="paxosMessageDeliver"></param>
        /// <param name="rpcRequestHandler"></param>
        public PaxosMessageHandler(IMessageDeliver paxosMessageDeliver, IRpcRequestHandler rpcRequestHandler)
        {
            _rpcRequestHandler = rpcRequestHandler;
            _messageDeliver = paxosMessageDeliver;
        }

        public async Task<RpcMessage> HandleRequest(RpcMessage request)
        {
            var paxosRpcMessage = PaxosRpcMessageFactory.CreatePaxosRpcMessage(request);
            switch (paxosRpcMessage.MessageType)
            {
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
                            foreach(var paxosMessage in aggregatedMsg.PaxosMessages)
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
                default:
                    if (_rpcRequestHandler != null)
                    {
                        var resp = await _rpcRequestHandler.HandleRequest(request);
                        return resp;
                    }
                    break;
            }

            return new RpcMessage()
            {
                IsRequest = false,
                RequestId = request.RequestId
            };
        }

    }
}
