using Paxos.Network;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace Paxos.Rpc
{
    public class RpcTransport : IRpcTransport
    {
        private IConnection _networkTransport;
        private IRpcRequestHandler _rpcRequestHandler;
        private Task _messageHandlerTask;
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>>();

        public RpcTransport(IConnection networkTransport)
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

    public class RpcClient : IRpcClient
    {
        private Task _messageHandlerTask;
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>>();
        private List<KeyValuePair<NodeAddress, IConnection>> _connections = new List<KeyValuePair<NodeAddress, IConnection>>();
        private List<NodeInfo> _pendingDeletionConnections = new List<NodeInfo>();
        private List<KeyValuePair<NodeAddress, IConnection>> _pendingAddConnections = new List<KeyValuePair<NodeAddress, IConnection>>();
        private TaskCompletionSource<RpcMessage> _signalTask = new TaskCompletionSource<RpcMessage>();
        private NodeAddress _localAddr;

        public RpcClient(NodeAddress localAddrr)
        {
            _localAddr = localAddrr;
            _messageHandlerTask = Task.Run(async () =>
            {
                var recvTaskList = new List<Task<RpcMessage>>();
                recvTaskList.Add(_signalTask.Task);
                foreach (var connection in _connections)
                {
                    recvTaskList.Add(connection.Value.ReceiveMessage());
                }
                while (!Stop)
                {
                    var signaledTask = await Task.WhenAny(recvTaskList);
                    int signalTaskIndex = 0;
                    foreach(var task in recvTaskList)
                    {
                        if (task.Id == signaledTask.Id)
                        {
                            break;
                        }
                        signalTaskIndex++;
                    }
                    if (signalTaskIndex == 0)
                    {
                        // signal event
                        // remove connections

                        // add new connections
                        lock (_pendingAddConnections)
                        {
                            foreach (var newConnection in _pendingAddConnections)
                            {
                                _connections.Add(newConnection);
                                recvTaskList.Add(newConnection.Value.ReceiveMessage());
                            }
                            _pendingAddConnections.Clear();
                            recvTaskList[0] = _signalTask.Task;
                        }
                    }
                    else if (signalTaskIndex > 0)
                    {
                        var rpcMessage = recvTaskList[signalTaskIndex].Result;
                        // find related request
                        TaskCompletionSource<RpcMessage> completionSource = null;
                        if (_ongoingRequests.TryRemove(rpcMessage.RequestId, out completionSource))
                        {
                            completionSource.SetResult(rpcMessage);
                        }

                        recvTaskList[signalTaskIndex] = _connections[signalTaskIndex - 1].Value.ReceiveMessage();
                    }

                }
            });
        }

        public async Task<RpcMessage> SendRequest(NodeAddress serverAddress, RpcMessage rpcRequest)
        {
            IConnection connection = null;
            lock (_pendingAddConnections)
            {
                connection = GetConnection(serverAddress);
            }

            if (connection == null)
            {
                connection = await NetworkFactory.CreateNetworkClient(_localAddr, serverAddress);
            }

            lock (_pendingAddConnections)
            {
                _pendingAddConnections.Add(new KeyValuePair<NodeAddress, IConnection>(serverAddress, connection));
                var oldSignalTask = _signalTask;
                _signalTask = new TaskCompletionSource<RpcMessage>();
                oldSignalTask.SetResult(new RpcMessage());

            }
            var completionSource = new TaskCompletionSource<RpcMessage>();
            _ongoingRequests.TryAdd(rpcRequest.RequestId, completionSource);

            await connection.SendMessage(rpcRequest);
            return await completionSource.Task;
        }

        public bool Stop { get; set; }

        private IConnection GetConnection(NodeAddress serverAddress)
        {
            {
                var connectionEntry = _connections.Where(connection => connection.Key.Node.Name.Equals(serverAddress.Node.Name)).FirstOrDefault();
                if (connectionEntry.Key != null && connectionEntry.Key.Node != null && !string.IsNullOrEmpty(connectionEntry.Key.Node.Name))
                {
                    return connectionEntry.Value;
                }
            }

            {
                var connectionEntry = _pendingAddConnections.Where(connection => connection.Key.Node.Name.Equals(serverAddress.Node.Name)).FirstOrDefault();
                if (connectionEntry.Key != null && connectionEntry.Key.Node != null && !string.IsNullOrEmpty(connectionEntry.Key.Node.Name))
                {
                    return connectionEntry.Value;
                }
            }

            return null;
        }
    }

    public class RpcServer : IRpcServer, IConnectionChangeNotification
    {
        private IRpcRequestHandler _rpcRequestHandler;
        private Task _messageHandlerTask;
        private List<IConnection> _connections = new List<IConnection>();
        private List<IConnection> _pendingAddConnections = new List<IConnection>();
        private TaskCompletionSource<RpcMessage> _signalTask = new TaskCompletionSource<RpcMessage>();
        private INetworkServer _networkServer;
        private NodeAddress _serverAddr;
        private bool _isStop = false;

        public RpcServer(NodeAddress localAddrr)
        {
            _serverAddr = localAddrr;
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
            _rpcRequestHandler = requestHandler;
        }

        public void OnNewConnection(IConnection connection)
        {
            lock(_pendingAddConnections)
            {
                _pendingAddConnections.Add(connection);

                var oldSignalTask = _signalTask;
                _signalTask = new TaskCompletionSource<RpcMessage>();
                oldSignalTask.SetResult(new RpcMessage());
            }
        }

        public async Task Start()
        {
            if (_networkServer == null)
            {
                _networkServer = await NetworkFactory.CreateNetworkServer(_serverAddr);
                _networkServer.SubscribeConnectionChangeNotification(this);
            }

            if (_messageHandlerTask == null)
            {

                _messageHandlerTask = Task.Run(async () =>
                {
                    var recvTaskList = new List<Task<RpcMessage>>();
                    recvTaskList.Add(_signalTask.Task);
                    foreach (var connection in _connections)
                    {
                        recvTaskList.Add(connection.ReceiveMessage());
                    }
                    while (!Stop)
                    {
                        var signaledTask = await Task.WhenAny(recvTaskList);
                        int signalTaskIndex = 0;
                        foreach(var task in recvTaskList)
                        {
                            if (task.Id == signaledTask.Id)
                            {
                                break;
                            }
                            signalTaskIndex++;
                        }
                        if (signalTaskIndex == 0)
                        {
                            // signal event
                            // remove connections

                            // add new connections
                            lock (_pendingAddConnections)
                            {
                                foreach (var newConnection in _pendingAddConnections)
                                {
                                    _connections.Add(newConnection);
                                    recvTaskList.Add(newConnection.ReceiveMessage());
                                }
                                _pendingAddConnections.Clear();
                                recvTaskList[0] = _signalTask.Task;
                            }
                        }
                        else if (signalTaskIndex > 0)
                        {
                            var rpcRequest = recvTaskList[signalTaskIndex].Result;
                            recvTaskList[signalTaskIndex] = _connections[signalTaskIndex - 1].ReceiveMessage();

                            var task = Task.Run(async () =>
                            {
                                RpcMessage rpcResp = null;
                                if (_rpcRequestHandler != null)
                                {
                                    rpcResp = await _rpcRequestHandler.HandleRequest(rpcRequest);
                                }
                                if (rpcResp == null)
                                {
                                    rpcResp = new RpcMessage()
                                    { IsRequest = false, RequestId = rpcRequest.RequestId, RequestContent = "" };
                                }

                                await _connections[signalTaskIndex - 1].SendMessage(rpcResp);
                            });
                        }

                    }
                });
            }
        }

        public bool Stop {
            get
            {
                return _isStop;
            }
            set
            {
                _isStop = value;
                if (_isStop)
                {
                    _signalTask.SetResult(new RpcMessage());
                }
            }
        }
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
