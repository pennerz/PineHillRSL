using Paxos.Network;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Rpc
{
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
}
