using Paxos.Network;
using Paxos.Common;
using Paxos.PerCounter;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
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
        private ConcurrentDictionary<NodeAddress, IConnection> _connectionsTable = new ConcurrentDictionary<NodeAddress, IConnection>();
        private List<NodeInfo> _pendingDeletionConnections = new List<NodeInfo>();
        private List<KeyValuePair<NodeAddress, IConnection>> _pendingAddConnections = new List<KeyValuePair<NodeAddress, IConnection>>();
        private TaskCompletionSource<List<NetworkMessage>> _signalTask = new TaskCompletionSource<List<NetworkMessage>>();
        private NodeAddress _localAddr;
        private IRpcNodeEventHandler _rpcEventHandler;
        private bool _isStop = false;
        private ConcurrentQueue<Tuple<IConnection, List<NetworkMessage>>> _networkMessageQueue = new ConcurrentQueue<Tuple<IConnection, List<NetworkMessage>>>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);
        private List<Task> _networkMessageProcessTask = new List<Task>();
        private bool _usePreallocateTaskPool = false;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        //private List<int> _lock = new List<int>();

        public RpcNode(NodeAddress localAddrr, IRpcNodeEventHandler rpcNodeEventHandler)
        {
            _localAddr = localAddrr;
            _rpcEventHandler = rpcNodeEventHandler;
            _messageHandlerTask = Task.Run(async () =>
            {
                await ProcessNetworkEvent();
            });

            if (_usePreallocateTaskPool)
            {
                for (int i = 0; i < 5; i++)
                {
                    _networkMessageProcessTask.Add(Task.Run(async () =>
                    {
                        while (!_isStop)
                        {
                            Tuple<IConnection, List<NetworkMessage>> receivedEvent = null;

                            await _receivedMessageSemaphore.WaitAsync();
                            if (!_networkMessageQueue.TryDequeue(out receivedEvent))
                            {
                                continue;
                            }
                            var taskList = new List<Task>();
                            foreach (var recvNetworkMsg in receivedEvent.Item2)
                            {
                                var task = Task.Run(async () =>
                                {
                                    var rpcMessage = RpcMessageHelper.CreateRpcMessage(recvNetworkMsg);
                                    await _rpcEventHandler.OnReceived(receivedEvent.Item1, rpcMessage);
                                });
                                taskList.Add(task);
                            }

                            await Task.WhenAll(taskList);
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
            var pendingConnection = new KeyValuePair<NodeAddress, IConnection>(remoteAddress, connection);
            var newSignalTask = new TaskCompletionSource<List<NetworkMessage>>();

            lock (_lock)
            {
                if (_connectionsTable.ContainsKey(remoteAddress))
                {
                    return;
                }
                _pendingAddConnections.Add(pendingConnection);
                _connectionsTable[remoteAddress] = connection;

                var oldSignalTask = _signalTask;
                _signalTask = newSignalTask;
                oldSignalTask.SetResult((List<NetworkMessage>)null);
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

            lock (_lock)
            {
                connection = GetConnectionInLock(remoteAddress);
                if (connection != null)
                {
                    return connection;
                }
            }

            connection = await NetworkFactory.CreateNetworkClient(_localAddr, remoteAddress);
            if (connection == null)
            {
                return connection;
            }
            var pendingConnection = new KeyValuePair<NodeAddress, IConnection>(remoteAddress, connection);
            var newSignalTask = new TaskCompletionSource<List<NetworkMessage>>();

            lock (_lock)
            {
                var oldconnection = GetConnectionInLock(remoteAddress);
                if (oldconnection == null)
                {
                    _pendingAddConnections.Add(pendingConnection);
                    _connectionsTable.TryAdd(remoteAddress, connection);
                    _connectionsTable[remoteAddress] = connection;
                    var oldSignalTask = _signalTask;
                    _signalTask = newSignalTask;
                    oldSignalTask.SetResult((List<NetworkMessage>)null);
                }
                else
                {
                    connection = oldconnection;
                }
            }

            return connection;
        }

        /// <summary>
        /// Core processor for network packet receiving tasks.
        /// </summary>
        /// <returns></returns>
        private async Task ProcessNetworkEvent()
        {
            var recvTaskList = new List<Task<List<NetworkMessage>>>();
            //recvTaskList.Add(_signalTask.Task);
            foreach (var connection in _connections)
            {
                recvTaskList.Add(connection.Value.ReceiveMessage());
            }

            var runningTasks = new List<Task>();
            for (int i = 0; i < recvTaskList.Count; i++)
            {
                var task = Task.Run(async () =>
                {
                    var signalTaskIndex = i;
                    while(!_isStop)
                    {
                        await recvTaskList[signalTaskIndex];
                        if (signalTaskIndex > 0)
                        {
                            var receivedConnection = _connections[signalTaskIndex].Value;
                            var receivedNetworkMessages = recvTaskList[signalTaskIndex].Result;
                            recvTaskList[signalTaskIndex] = _connections[signalTaskIndex].Value.ReceiveMessage();
                            {
                                var task = Task.Run(async () =>
                                {
                                    var taskList = new List<Task>();
                                    if (receivedNetworkMessages.Count > 10)
                                    {
                                        //Console.WriteLine("received message {0}", receivedNetworkMessages.Count);
                                    }
                                    foreach (var recvNetworkMsg in receivedNetworkMessages)
                                    {
                                        var delayedReceivedTime = DateTime.Now - recvNetworkMsg.DeliveredTime;
                                        if (delayedReceivedTime.TotalMilliseconds > 100)
                                        {
                                            //Console.WriteLine("too slow");
                                        }
                                        var task = Task.Run(async () =>
                                        {
                                            var rpcMessage = RpcMessageHelper.CreateRpcMessage(recvNetworkMsg);
                                            await _rpcEventHandler.OnReceived(receivedConnection, rpcMessage);
                                        });
                                        taskList.Add(task);
                                    }

                                    await Task.WhenAll(taskList);
                                });
                            }
                        }

                    }

                });

                runningTasks.Add(task);
            }

            while (!_isStop)
            {
                await _signalTask.Task;

                // signal event
                MergeChangedConnection(recvTaskList);

                for (int i = runningTasks.Count; i < recvTaskList.Count; i++)
                {
                    var signalTaskIndex = i;
                    using (var counter = new PerfTimerCounter((int)NetworkPerfCounterType.NetworkMessageProcessTaskCreationTime))
                    {
                        var task = Task.Run(async () =>
                        {
                            while (!_isStop)
                            {
                                using (var recvCounter = new PerfTimerCounter(
                                    (int)NetworkPerfCounterType.NetworkMessageRecvWaitTime))
                                {
                                    await recvTaskList[signalTaskIndex];
                                }

                                //if (signalTaskIndex > 0)
                                {
                                    var receivedConnection = _connections[signalTaskIndex].Value;
                                    var receivedNetworkMessages = recvTaskList[signalTaskIndex].Result;

                                    StatisticCounter.ReportCounter(
                                        (int)NetworkPerfCounterType.NetworkMessageBatchCount,
                                        receivedNetworkMessages.Count);

                                    recvTaskList[signalTaskIndex] = _connections[signalTaskIndex].Value.ReceiveMessage();
                                    {
                                        var taskCreateTime = DateTime.Now;
                                        //var task = Task.Run(async () =>
                                        {
                                            var taskDelayedTime = DateTime.Now - taskCreateTime;
                                            if (taskDelayedTime.TotalMilliseconds > 10)
                                            {
                                                //Console.WriteLine("Task delayed time {0}", taskDelayedTime.TotalMilliseconds);
                                            }
                                            using (var messageProcessTimer = new PerfTimerCounter(
                                                (int)NetworkPerfCounterType.NetworkMessageProcessTime))
                                            {
                                                var taskList = new List<Task>();
                                                if (receivedNetworkMessages.Count > 10)
                                                {
                                                    //Console.WriteLine("received message {0}", receivedNetworkMessages.Count);
                                                }
                                                foreach (var recvNetworkMsg in receivedNetworkMessages)
                                                {
                                                    var delayedReceivedTime = DateTime.Now - recvNetworkMsg.DeliveredTime;
                                                    if (delayedReceivedTime.TotalMilliseconds > 100)
                                                    {
                                                        //Console.WriteLine("too slow");
                                                    }

                                                    var task = Task.Run(async () =>
                                                    {
                                                        var timeInReceivedQueue = DateTime.Now - recvNetworkMsg.DeliveredTime;
                                                        if (timeInReceivedQueue.TotalMilliseconds > 10)
                                                        {
                                                            var handleDelaedTime = DateTime.Now - recvNetworkMsg.ReceivedTime;
                                                            //Console.WriteLine("received message delayed time in ms{0}", timeInReceivedQueue.TotalMilliseconds);
                                                        }
                                                        StatisticCounter.ReportCounter((int)NetworkPerfCounterType.ConcurrentNetworkTaskCount, 1);
                                                        var rpcMessage = RpcMessageHelper.CreateRpcMessage(recvNetworkMsg);
                                                        await _rpcEventHandler.OnReceived(receivedConnection, rpcMessage);
                                                    });
                                                    taskList.Add(task);
                                                }

                                                await Task.WhenAll(taskList);
                                                StatisticCounter.ReportCounter((int)NetworkPerfCounterType.ConcurrentNetworkTaskCount, -taskList.Count);
                                            }
                                            //});
                                        }
                                    }


                                }
                            }
                        });
                        runningTasks.Add(task);
                    }

                }
            }

            /*
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
                    var receivedNetworkMessages = recvTaskList[signalTaskIndex].Result;
                    recvTaskList[signalTaskIndex] = _connections[signalTaskIndex - 1].Value.ReceiveMessage();
                    if (_usePreallocateTaskPool)
                    {
                        var networkEvent = new Tuple<IConnection, List<NetworkMessage>>(receivedConnection, receivedNetworkMessages);
                        _networkMessageQueue.Enqueue(networkEvent);
                        _receivedMessageSemaphore.Release();
                    }
                    else
                    {
                        var task = Task.Run(async() =>
                        {
                            var taskList = new List<Task>();
                            if (receivedNetworkMessages.Count > 10)
                            {
                                Console.WriteLine("received message {0}", receivedNetworkMessages.Count);
                            }
                            foreach(var recvNetworkMsg in receivedNetworkMessages)
                            {
                                var task = Task.Run(async () =>
                                {
                                    var rpcMessage = RpcMessageHelper.CreateRpcMessage(recvNetworkMsg);
                                    await _rpcEventHandler.OnReceived(receivedConnection, rpcMessage);
                                });
                                taskList.Add(task);
                            }

                            await Task.WhenAll(taskList);
                        });
                    }
                }
            }*/
        }

        /// <summary>
        /// get a connection to remote address. The caller must get the lock for
        /// connection management.
        /// </summary>
        /// <param name="serverAddress"></param>
        /// <returns></returns>
        private IConnection GetConnectionInLock(NodeAddress serverAddress)
        {
            IConnection connection = null;
            if (!_connectionsTable.TryGetValue(serverAddress, out connection))
            {
                return null;
            }
            return connection;
            //return _connectionsTable[serverAddress];

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
        private void MergeChangedConnection(List<Task<List<NetworkMessage>>> recvTaskList)
        {
            lock (_lock)
            {
                // remove connections

                // add new connections

                foreach (var newConnection in _pendingAddConnections)
                {
                    _connections.Add(newConnection);
                    recvTaskList.Add(newConnection.Value.ReceiveMessage());
                }
                _pendingAddConnections.Clear();
                //recvTaskList[0] = _signalTask.Task;
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
            IConnection connection = null;
            
            try
            {
                connection = await _node.GetConnection(serverAddress);
            }
            catch(Exception)
            {

            }
            if (connection == null)
            {
                return null;
            }

            if (rpcRequest.NeedResp)
            {
                var completionSource = new TaskCompletionSource<RpcMessage>();
                _ongoingRequests.TryAdd(rpcRequest.RequestId, completionSource);
                var networkMessage = RpcMessageHelper.CreateNetworkMessage(rpcRequest);
                await connection.SendMessage(networkMessage);
                return await completionSource.Task;
            }
            else
            {
                var networkMessage = RpcMessageHelper.CreateNetworkMessage(rpcRequest);
                await connection.SendMessage(networkMessage);
                return null;
            }

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

            if (rpcRequest.NeedResp)
            {
                if (rpcResp == null)
                {
                    rpcResp = new RpcMessage()
                    { IsRequest = false, RequestId = rpcRequest.RequestId, RequestContent = null };
                }
                else
                {
                    rpcResp.IsRequest = false;
                    rpcResp.RequestId = rpcRequest.RequestId;
                }

                await connection.SendMessage(RpcMessageHelper.CreateNetworkMessage(rpcResp));
            }
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
