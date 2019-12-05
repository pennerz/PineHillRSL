using Paxos.Network;
using Paxos.MessageDelivery;
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
        StaleBallot
    }

    public class PaxosRpcMessage
    {
        public PaxosRpcMessageType MessageType { get; set; }
        public string MessageContent { get; set; }
    }

    public class PaxosMessageHandler : IRpcRequestHandler
    {
        private IMessageDelivery _messageDeliver;
        private IRpcRequestHandler _rpcRequestHandler;

        public PaxosMessageHandler(IMessageDelivery paxosMessageDeliver, IRpcRequestHandler rpcRequestHandler)
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
                    {
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
                    }
                    break;
                default:
                    {
                        if (_rpcRequestHandler != null)
                        {
                            var resp = await _rpcRequestHandler.HandleRequest(request);
                            return resp;
                        }
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
