using Paxos.Message;
using Paxos.MessageDelivery;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Paxos.Network
{
    public class NodeInfo
    {
        public string Name { get; set; }

    }

    public class RpcMessage
    {
        public RpcMessage()
        {
            RequestId = Guid.NewGuid();
            IsRequest = true;
        }

        public Guid RequestId { get; set; }
        public bool IsRequest { get; set; }
        public string RequestContent { get; set; }
    }

    public interface IRpcRequestHandler
    {
        Task<RpcMessage> HandleRequest(RpcMessage request);
    }



    //TODO: abstract the PaxosMessage
    /*
    public interface IMessageTransport
    {
        Task SendMessage(PaxosMessage msg);

        Task<PaxosMessage> ReceiveMessage();
    }

    public class MessageTransport : IMessageTransport
    {
        private List<PaxosMessage> _receivedPaxosMessage;

        public Task SendMessage(PaxosMessage paxosMessage)
        {
            return Task.CompletedTask;
        }

        public Task<PaxosMessage> ReceiveMessage()
        {
            return Task.FromResult((PaxosMessage)null);
        }

        public void DeliverMessage(PaxosMessage msg)
        {
            _receivedPaxosMessage.Add(msg);
        }
    }*/

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

    public class PaxosMessageFactory
    {
        public static PaxosMessage CreatePaxosMessage(PaxosRpcMessage rpcMessage)
        {
            switch(rpcMessage.MessageType)
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

    /*
    public interface IRpcMessageChannel
    {
        Task Send(RpcMessage rpcMessage);
        Task<RpcMessage> Receive();
    }

    public class PaxosRpcNode
    {
        private IDictionary<string, IRpcMessageChannel> _rpcConnections;
        private IRpcRequestHandler _requestHandler = null;
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcResponse>>();
        private Task _messageHandlerTask;

        public PaxosRpcNode(
            IDictionary<string, IRpcMessageChannel> rpcConnections,
            IRpcRequestHandler rpcRequestHandler)
        {
            _rpcConnections = rpcConnections;
            _requestHandler = rpcRequestHandler;

            Stop = false;

            _messageHandlerTask = Task.Run(async () =>
            {
                var allConnectionTasks = new List<Task<RpcMessage>>();
                foreach (var connection in _rpcConnections)
                {
                    var task = connection.Value.Receive();
                    allConnectionTasks.Add(task);

                }
                while (!Stop)
                {
                    var rpcMessageTask = await Task.WhenAny(allConnectionTasks);
                    int taskIndex = 0;
                    string targetConnection = "";
                    foreach (var connection in _rpcConnections)
                    {
                        if (allConnectionTasks[taskIndex] == rpcMessageTask)
                        {
                            targetConnection = connection.Key;
                            break;
                        }
                        taskIndex++;
                    }
                    allConnectionTasks.RemoveAt(taskIndex);
                    allConnectionTasks.Add(_rpcConnections[targetConnection].Receive());
                    var rpcMessage = rpcMessageTask.Result;
                    if (rpcMessage.IsRequest)
                    {
                        if (_requestHandler != null)
                        {
                            var resp = await _requestHandler.HandleRequest((RpcRequest)rpcMessage);
                            await _rpcConnections[targetConnection].Send(resp);
                        }
                    }
                    else
                    {
                        // find related request
                        TaskCompletionSource<RpcResponse> completionSource = null;
                        if (_ongoingRequests.TryRemove(rpcMessage.RequestId, out completionSource))
                        {
                            completionSource.SetResult((RpcResponse)rpcMessage);
                        }

                    }
                }
            });

        }

        public async Task<RpcMessage> SendRequest(string targetNode, RpcMessage request)
        {
            request.RequestId = Guid.NewGuid();
            IRpcMessageChannel rpcConnection = null;
            if (!_rpcConnections.TryGetValue(targetNode, out rpcConnection))
            {
                return null;
            }

            var completionSource = new TaskCompletionSource<RpcMessage>();
            while (!_ongoingRequests.TryAdd(request.RequestId, completionSource)) ;

            // send the request
            await rpcConnection.Send(request);
            var response = await completionSource.Task;
            return response;
        }

        public bool Stop { get; set; }

    }
    */

    public interface IRpcTransport
    {
        Task<RpcMessage> SendRequest(RpcMessage request);
        void RegisterRequestHandler(IRpcRequestHandler requestHandler);
    }

    public interface INetworkTransport
    {
        Task SendMessage(RpcMessage msg);

        Task<RpcMessage> ReceiveMessage();
    }

    public class RpcTransport : IRpcTransport
    {
        private INetworkTransport _networkTransport;
        private IRpcRequestHandler _rpcRequestHandler;
        private Task _messageHandlerTask;
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>>();

        public RpcTransport(INetworkTransport networkTransport)
        {
            _networkTransport = networkTransport;

            _messageHandlerTask = Task.Run(async () =>
            {
                while (!Stop)
                {
                    var rpcMessage = await _networkTransport.ReceiveMessage();

                    var task = Task.Run(async () =>
                    {
                        if (rpcMessage.IsRequest)
                        {
                            if (_rpcRequestHandler != null)
                            {
                                var resp = await _rpcRequestHandler.HandleRequest(rpcMessage);
                                await _networkTransport.SendMessage(resp);
                            }
                        }
                        else
                        {
                            // find related request
                            TaskCompletionSource<RpcMessage> completionSource = null;
                            if (_ongoingRequests.TryRemove(rpcMessage.RequestId, out completionSource))
                            {
                                completionSource.SetResult(rpcMessage);
                            }
                        }

                    });
                }
            });
        }

        public async Task<RpcMessage> SendRequest(RpcMessage rpcRequest)
        {
            var completionSource = new TaskCompletionSource<RpcMessage>();
            _ongoingRequests.TryAdd(rpcRequest.RequestId, completionSource);
            await _networkTransport.SendMessage(rpcRequest);
            return await completionSource.Task;
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
            _rpcRequestHandler = requestHandler;
        }

        public bool Stop { get; set; }
    }


    public class NetworkServer
    {
        private ConcurrentDictionary<string, IRpcTransport> _rpcConnections = new ConcurrentDictionary<string, IRpcTransport>();
        string _node;
        public NetworkServer(string node, ConcurrentDictionary<string, IRpcTransport> rpcConnections)
        {
            _node = node;
            if (rpcConnections != null)
            {
                _rpcConnections = rpcConnections;
            }
        }

        public IRpcTransport GetRpcTransport(string targetNode)
        {
            IRpcTransport rpcTransport = null;
            if (_rpcConnections.TryGetValue(targetNode, out rpcTransport))
            {
                return rpcTransport;
            }
            return null;
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
            foreach(var connection in _rpcConnections)
            {
                connection.Value.RegisterRequestHandler(requestHandler);
            }
        }

    }

    /*
    class PaxosRpcTransport : IRpcTransport
    {
        private IRpcRequestHandler _requestHandler = null;
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcResponse>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcResponse>>();

        public PaxosRpcTransport()
        {

        }


        public Task<RpcResponse> SendRequest(RpcRequest request)
        {
            request.RequestId = Guid.NewGuid();
            var completionSource = new TaskCompletionSource<RpcResponse>();
            while (!_ongoingRequests.TryAdd(request.RequestId, completionSource));

            // send the request

            return completionSource.Task;
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
        }

    }*/
}
