using Paxos.Network;
using Paxos.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace Paxos.Rpc
{
    /// <summary>
    /// Rpc node event handler, which has method to handle different kind
    /// events, such as message received.
    /// </summary>
    interface IRpcNodeEventHandler
    {
        Task OnReceived(IConnection connection, RpcMessage rpcMessage);
    }


    /// <summary>
    /// Internal class which handle message receiving task, and connection
    /// managements
    /// This rpc client & server delegate their message receiving & connections
    /// managemnt task to it.
    /// </summary>
    class RpcNode
    {
        private Task _messageHandlerTask;
        private List<KeyValuePair<NodeAddress, IConnection>> _connections = new List<KeyValuePair<NodeAddress, IConnection>>();
        private List<NodeInfo> _pendingDeletionConnections = new List<NodeInfo>();
        private List<KeyValuePair<NodeAddress, IConnection>> _pendingAddConnections = new List<KeyValuePair<NodeAddress, IConnection>>();
        private TaskCompletionSource<NetworkMessage> _signalTask = new TaskCompletionSource<NetworkMessage>();
        private NodeAddress _localAddr;
        private IRpcNodeEventHandler _rpcEventHandler;
        private bool _isStop = false;
        private ConcurrentQueue<Tuple<IConnection, NetworkMessage>> _networkMessageQueue = new ConcurrentQueue<Tuple<IConnection, NetworkMessage>>();
        private List<Task> _networkMessageProcessTask = new List<Task>();

        public RpcNode(NodeAddress localAddrr, IRpcNodeEventHandler rpcNodeEventHandler)
        {
            _localAddr = localAddrr;
            _rpcEventHandler = rpcNodeEventHandler;
            _messageHandlerTask = Task.Run(async () =>
            {
                await ProcessNetworkEvent();
            });

            if (false)
            {
                for (int i = 0; i < 3; i++)
                {
                    _networkMessageProcessTask.Add(Task.Run(async () =>
                    {
                        while (!_isStop)
                        {
                            Tuple<IConnection, NetworkMessage> receivedEvent = null;
                            if (!_networkMessageQueue.TryDequeue(out receivedEvent))
                            {
                                await Task.Delay(10);
                                continue;
                            }
                            var rpcMessage = RpcMessageHelper.CreateRpcMessage(receivedEvent.Item2);
                            await _rpcEventHandler.OnReceived(receivedEvent.Item1, rpcMessage);
                        }
                    }));

                }
            }
        }

        public async Task Stop()
        {
            _isStop = true;
            if (_messageHandlerTask != null)
            {
                await _messageHandlerTask;
            }
        }

        /// <summary>
        /// Add a new connection to the node.
        /// </summary>
        /// <param name="connection"></param>
        public void AddConnection(IConnection connection)
        {
            NodeAddress remoteAddress = connection.RemoteAddress;
            lock (_pendingAddConnections)
            {
                _pendingAddConnections.Add(new KeyValuePair<NodeAddress, IConnection>(remoteAddress, connection));
                var oldSignalTask = _signalTask;
                _signalTask = new TaskCompletionSource<NetworkMessage>();
                oldSignalTask.SetResult(new NetworkMessage());
            }
        }

        /// <summary>
        /// Get a connection to the remote address. If not exist, create one.
        /// </summary>
        /// <param name="remoteAddress"></param>
        /// <returns></returns>
        public async Task<IConnection> GetConnection(NodeAddress remoteAddress)
        {
            IConnection connection = null;
            lock (_pendingAddConnections)
            {
                connection = GetConnectionInLock(remoteAddress);

            }

            if (connection == null)
            {
                connection = await NetworkFactory.CreateNetworkClient(_localAddr, remoteAddress);
            }

            lock (_pendingAddConnections)
            {
                var oldConnection = GetConnectionInLock(remoteAddress);
                if (oldConnection != null)
                {
                    connection = oldConnection;
                }
                else
                {
                    _pendingAddConnections.Add(new KeyValuePair<NodeAddress, IConnection>(remoteAddress, connection));
                }
                var oldSignalTask = _signalTask;
                _signalTask = new TaskCompletionSource<NetworkMessage>();
                oldSignalTask.SetResult(new NetworkMessage());
            }

            return connection;
        }

        /// <summary>
        /// Core processor for network packet receiving tasks.
        /// </summary>
        /// <returns></returns>
        private async Task ProcessNetworkEvent()
        {
            var recvTaskList = new List<Task<NetworkMessage>>();
            recvTaskList.Add(_signalTask.Task);
            foreach (var connection in _connections)
            {
                recvTaskList.Add(connection.Value.ReceiveMessage());
            }
            while (!_isStop)
            {
                var signaledTask = await Task.WhenAny(recvTaskList);
                int signalTaskIndex = 0;
                foreach (var task in recvTaskList)
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
                    MergeChangedConnection(recvTaskList);
                }
                else if (signalTaskIndex > 0)
                {
                    var receivedConnection = _connections[signalTaskIndex - 1].Value;
                    var receivedNetworkMessage = recvTaskList[signalTaskIndex].Result;
                    recvTaskList[signalTaskIndex] = _connections[signalTaskIndex - 1].Value.ReceiveMessage();
                    //var oldSignalTask = _signalTask;
                    //_signalTask = new TaskCompletionSource<NetworkMessage>();
                    if (false)
                    {
                        var networkEvent = new Tuple<IConnection, NetworkMessage>(receivedConnection, receivedNetworkMessage);
                        _networkMessageQueue.Enqueue(networkEvent);
                    }
                    else
                    {
                        var task = Task.Run(async() =>
                        {
                            var rpcMessage = RpcMessageHelper.CreateRpcMessage(receivedNetworkMessage);
                            await _rpcEventHandler.OnReceived(receivedConnection, rpcMessage);
                            //recvTaskList[signalTaskIndex] = _connections[signalTaskIndex - 1].Value.ReceiveMessage();
                            //oldSignalTask.SetResult(new NetworkMessage());
                        });
                    }
                }
            }
        }

        /// <summary>
        /// get a connection to remote address. The caller must get the lock for
        /// connection management.
        /// </summary>
        /// <param name="serverAddress"></param>
        /// <returns></returns>
        private IConnection GetConnectionInLock(NodeAddress serverAddress)
        {
            var connectionEntry = _connections.Where(connection => connection.Key.Node.Name.Equals(serverAddress.Node.Name)).FirstOrDefault();
            if (connectionEntry.Key != null && connectionEntry.Key.Node != null && !string.IsNullOrEmpty(connectionEntry.Key.Node.Name))
            {
                return connectionEntry.Value;
            }
            connectionEntry = _pendingAddConnections.Where(connection => connection.Key.Node.Name.Equals(serverAddress.Node.Name)).FirstOrDefault();
            if (connectionEntry.Key != null && connectionEntry.Key.Node != null && !string.IsNullOrEmpty(connectionEntry.Key.Node.Name))
            {
                return connectionEntry.Value;
            }

            return null;
        }

        /// <summary>
        /// Merge the changed connection
        /// </summary>
        /// <param name="recvTaskList"></param>
        private void MergeChangedConnection(List<Task<NetworkMessage>> recvTaskList)
        {
            lock (_pendingAddConnections)
            {
                // remove connections

                // add new connections

                foreach (var newConnection in _pendingAddConnections)
                {
                    _connections.Add(newConnection);
                    recvTaskList.Add(newConnection.Value.ReceiveMessage());
                }
                _pendingAddConnections.Clear();
                recvTaskList[0] = _signalTask.Task;
            }
        }
    }

    /// <summary>
    /// Rpc client, used by components which need to generate rpc request.
    /// </summary>
    public class RpcClient : IRpcClient, IRpcNodeEventHandler
    {
        private ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>> _ongoingRequests = new ConcurrentDictionary<Guid, TaskCompletionSource<RpcMessage>>();
        private NodeAddress _localAddr;
        private RpcNode _node;

        public RpcClient(NodeAddress localAddrr)
        {
            _localAddr = localAddrr;
            _node = new RpcNode(_localAddr, this);
        }

        /// <summary>
        /// Send a rpc reqeust, and wait for the response.
        /// </summary>
        /// <param name="serverAddress"></param>
        /// <param name="rpcRequest"></param>
        /// <returns></returns>
        public async Task<RpcMessage> SendRequest(NodeAddress serverAddress, RpcMessage rpcRequest)
        {
            IConnection connection = await _node.GetConnection(serverAddress);
            if (connection == null)
            {
                return null;
            }

            var completionSource = new TaskCompletionSource<RpcMessage>();
            _ongoingRequests.TryAdd(rpcRequest.RequestId, completionSource);
            var networkMessage = RpcMessageHelper.CreateNetworkMessage(rpcRequest);
            await connection.SendMessage(networkMessage);
            return await completionSource.Task;
        }

        /// <summary>
        /// Stop the client.
        /// </summary>
        /// <returns></returns>
        public async Task Stop()
        {
            await _node.Stop();
        }


        /// <summary>
        /// Rpc message received callback.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="rpcMessage"></param>
        /// <returns></returns>
        public Task OnReceived(IConnection connection, RpcMessage rpcMessage)
        {
            var requestId = rpcMessage.RequestId;

            TaskCompletionSource<RpcMessage> completionSource = null;
            if (_ongoingRequests.TryRemove(rpcMessage.RequestId, out completionSource))
            {
                completionSource.SetResult(rpcMessage);
            }

            return Task.CompletedTask;
        }

    }


    /// <summary>
    /// Rpc server used by components which acts as a rpc server.
    /// </summary>
    public class RpcServer : IRpcServer, IRpcNodeEventHandler, IConnectionChangeNotification
    {
        private IRpcRequestHandler _rpcRequestHandler;
        private INetworkServer _networkServer;
        private NodeAddress _serverAddr;
        private RpcNode _node;

        public RpcServer(NodeAddress localAddrr)
        {
            _serverAddr = localAddrr;
            _node = new RpcNode(_serverAddr, this);
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
            _rpcRequestHandler = requestHandler;
        }

        /// <summary>
        /// Start the rpc server.
        /// </summary>
        /// <returns></returns>
        public async Task Start()
        {
            if (_networkServer == null)
            {
                _networkServer = await NetworkFactory.CreateNetworkServer(_serverAddr);
                _networkServer.SubscribeConnectionChangeNotification(this);
            }
        }

        /// <summary>
        /// Stop the rpc server.
        /// </summary>
        /// <returns></returns>
        public async Task Stop()
        {
           await _networkServer.StopServer();
        }

        /// <summary>
        /// One connection build callback
        /// </summary>
        /// <param name="connection"></param>
        public void OnNewConnection(IConnection connection)
        {
            _node.AddConnection(connection);
        }

        /// <summary>
        /// rpc request received callback.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="rpcRequest"></param>
        /// <returns></returns>
        public async Task OnReceived(IConnection connection, RpcMessage rpcRequest)
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

            await connection.SendMessage(RpcMessageHelper.CreateNetworkMessage(rpcResp));
        }
    }

    /// <summary>
    /// Helper to convert rpc message and network message.
    /// </summary>
    public class RpcMessageHelper
    {
        public static NetworkMessage CreateNetworkMessage(RpcMessage rpcMessage)
        {
            var networkMessage = new NetworkMessage();
            networkMessage.Data = Serializer<RpcMessage>.Serialize(rpcMessage);
            return networkMessage;
        }

        public static RpcMessage CreateRpcMessage(NetworkMessage networkMessage)
        {
            return Serializer<RpcMessage>.Deserialize(networkMessage.Data);
        }
    }
}
