using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace PineHillRSL.Tests
{
    /// <summary>
    /// Test implementation of IConnection.
    /// It will not really build connection, but create pair of connection object
    /// in the TestNetworkInfr. When message is sent, it will find the pair
    /// connection object and deliver the message to it direclty.
    /// </summary>
    public class TestConnection : IConnection
    {
        private readonly TestNetworkInfr _networkInfr;
        private readonly NodeAddress _localAddress;
        private readonly NodeAddress _remoteAddress;

        private TestConnection _remoteConnection = null;

        private List<NetworkMessage> _receivedMessages = new List<NetworkMessage>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);

        private bool _shutdown = false;

        static Statistic _receivedLockWaitTime = new Statistic();
        static Statistic _lockOccupiedTime = new Statistic();
        static Statistic _receivedMessageUpdateTime = new Statistic();
        static Statistic _delevieryLockWaitTime = new Statistic();
        static Statistic _delevieryOccupiedTime = new Statistic();
        static Statistic _semaphoreReleaseTime = new Statistic();
        static Statistic _delevieryTime = new Statistic();
        static Statistic _sendDelayedTime = new Statistic();

        //private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private List<int> _lock = new List<int>();

        private Random _random = new Random();

        public TestConnection(NodeAddress localAddr, NodeAddress remoteAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _remoteAddress = remoteAddr;
            _networkInfr = networkInfr;
        }

        public Task SendMessage(NetworkMessage msg)
        {
            if (_shutdown)
            {
                throw new Exception("shutdown");
            }

            using (var sendMsgTimer = new PerfTimerCounter((int)TestPerfCounterType.SendMessageTime))
            {
                if (_remoteConnection == null)
                {
                    _remoteConnection = _networkInfr.GetConnection(_remoteAddress, _localAddress);

                }
                var begin = DateTime.Now;
                var delayMs = _random.Next(3, 5);
                //await Task.Delay(delayMs);
                var delayedTime = DateTime.Now - begin;
                _sendDelayedTime.Accumulate(delayedTime.TotalMilliseconds);
                _remoteConnection.DeliverMessage(msg);
                var sentTime = DateTime.Now - begin;
                _delevieryTime.Accumulate(sentTime.TotalMilliseconds);
                if (sentTime.TotalMilliseconds > 1000)
                {
                    Console.WriteLine("connection deliver message cost too much time[{0}ms]", sentTime.TotalMilliseconds);
                }
            }

            return Task.CompletedTask;
        }


        public async Task<List<NetworkMessage>> ReceiveMessage()
        {
            if (_shutdown)
            {
                throw new Exception("shutdown");
            }

            var newMsgList = new List<NetworkMessage>();
            List<NetworkMessage> result = null;
            do
            {
                await _receivedMessageSemaphore.WaitAsync();

                DateTime beforeLock = DateTime.Now;
                lock (_lock)
                {
                    DateTime afterLock = DateTime.Now;
                    _receivedLockWaitTime.Accumulate((afterLock - beforeLock).TotalMilliseconds);
                    if (_receivedMessages.Count > 0)
                    {
                        //var message = _receivedMessages[0];
                        //_receivedMessages.RemoveAt(0);
                        // return message;
                         result = _receivedMessages;
                        _receivedMessages = newMsgList;
                        _lockOccupiedTime.Accumulate((DateTime.Now - afterLock).TotalMilliseconds);
                        break;
                    }
                    _lockOccupiedTime.Accumulate((DateTime.Now - afterLock).TotalMilliseconds);
                }
            } while (true);

            var now = DateTime.Now;
            foreach (var msg in result)
            {
                msg.ReceivedTime = now;
            }
            _receivedMessageUpdateTime.Accumulate((DateTime.Now - now).TotalMilliseconds);
            return result;
        }

        /// <summary>
        /// Wait for all received message to be consumed.
        /// </summary>
        /// <returns></returns>
        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            do
            {
                lock (_lock)
                {
                    if (_receivedMessages.Count == 0)
                    {
                        return;
                    }
                }

                await Task.Delay(50);

            } while (true);
        }

        /// <summary>
        /// Deliver the message to received message queue.
        /// </summary>
        /// <param name="message"></param>
        public void DeliverMessage(NetworkMessage message)
        {
            if (_shutdown)
            {
                throw new Exception("shutdown");
            }

            DateTime beforeLock = DateTime.Now;
            lock (_lock)
            {
                var afterLock = DateTime.Now;
                _delevieryLockWaitTime.Accumulate((afterLock - beforeLock).TotalMilliseconds);
                message.DeliveredTime = DateTime.Now;
                _receivedMessages.Add(message);
                _delevieryOccupiedTime.Accumulate((DateTime.Now - afterLock).TotalMilliseconds);
            }
            DateTime beforeSemaphore = DateTime.Now;
            _receivedMessageSemaphore.Release();
            _semaphoreReleaseTime.Accumulate((DateTime.Now - beforeSemaphore).TotalMilliseconds);
        }

        public void Shutdown()
        {
            _shutdown = true;
        }

        public NodeAddress LocalAddress => _localAddress;

        public NodeAddress RemoteAddress => _remoteAddress;
    }

    /// <summary>
    /// A container for one node's connections
    /// </summary>
    public class TestNodeConnections
    {
        private NodeAddress _localAddress;
        private ConcurrentDictionary<NodeAddress, TestConnection> _connections =
            new ConcurrentDictionary<NodeAddress, TestConnection>();
        private TestNetworkInfr _networkInfr = null;

        public TestNodeConnections(NodeAddress localAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _networkInfr = networkInfr;
        }

        public TestConnection AddConnection(NodeAddress remoteAddress)
        {
            //lock(_connections)
            {
                TestConnection connection = null;
                if (_connections.TryGetValue(remoteAddress, out connection))
                {
                    return connection;
                }
                connection = new TestConnection(
                    _localAddress, remoteAddress, _networkInfr);

                _connections.TryAdd(remoteAddress, connection);
                return connection;
            }
        }
    }

    /// <summary>
    /// Network map which include all the nodes' connections. Test connection
    /// rely on it to find the right connection to deliver the message.
    /// </summary>
    public class TestNetworkInfr
    {
        List<TestConnection> _connections = new List<TestConnection>();
        List<TestNetworkServer> _servers = new List<TestNetworkServer>();
        ConcurrentDictionary<int, List<TestConnection>> _connectionIndex = new ConcurrentDictionary<int, List<TestConnection>>();

        //public List<TestConnection> NetworkConnections  => _connections;
        private bool _useConnectionMap = true;
        public void AddConnection(TestConnection connection)
        {
            if (_useConnectionMap)
            {
                List<TestConnection> connectionList = null;
                var hasVal = connection.LocalAddress.GetHashCode() + connection.RemoteAddress.GetHashCode() * 10;
                if (!_connectionIndex.TryGetValue(hasVal, out connectionList))
                {
                    connectionList = _connectionIndex.GetOrAdd(hasVal, new List<TestConnection>());
                }
                lock(connectionList)
                {
                    connectionList.Add(connection);
                }
            }
            else
            {
                lock (_connections)
                {
                    _connections.Add(connection);
                }
            }
        }

        public List<TestNetworkServer> NetworkServers => _servers;

        public TestConnection GetConnection(NodeAddress localAddr, NodeAddress remoteAddr)
        {
            using (var findConnectionTimer = new PerfTimerCounter((int)TestPerfCounterType.GetConnectionTime))
            {
                if (_useConnectionMap)
                {
                    List<TestConnection> connectionList = null;
                    var hasVal = localAddr.GetHashCode() + remoteAddr.GetHashCode() * 10;

                    if (!_connectionIndex.TryGetValue(hasVal, out connectionList))
                    {
                        return null;
                    }
                    if (connectionList == null)
                    {
                        return null;
                    }
                    else
                    {
                        lock(connectionList)
                        {
                            foreach(var connection in connectionList)
                            {
                                if (connection.LocalAddress.Equals(localAddr) &&
                                    connection.RemoteAddress.Equals(remoteAddr))
                                {
                                    return connection;
                                }
                            }
                            return null;
                        }
                    }
                }
                else
                {
                    lock (_connections)
                    {
                        var connectionList = _connections.Where(connection =>
                                connection.LocalAddress.Equals(localAddr) &&
                                connection.RemoteAddress.Equals(remoteAddr));
                        if (connectionList.Count() == 0)
                        {
                            return null;
                        }
                        return connectionList.First();
                    }
                }
            }
        }

        public TestNetworkServer GetServer(NodeAddress serverAddr)
        {
            lock(_servers)
            {
                var serverList = _servers.Where(server =>
                    server.ServerAddress.Equals(serverAddr));
                if (serverList.Count() == 0)
                {
                    return null;
                }
                return serverList.First();
            }
        }

        public void ShutdownServerConnection(NodeAddress serverAddr)
        {
            if (_useConnectionMap)
            {
                //for (int i = 0; i < _connectionIndex.Count; ++i)
                foreach(var item in _connectionIndex)
                {
                    var connectionList = item.Value;
                    if (connectionList == null)
                    {
                        continue;
                    }
                    lock (connectionList)
                    {
                        var removedConnections = new List<TestConnection>();
                        foreach(var connection in connectionList)
                        {
                            if (connection.RemoteAddress.Equals(serverAddr))
                            {
                                connection.Shutdown();
                                removedConnections.Add(connection);
                            }
                        }
                        foreach (var removedConnection in removedConnections)
                        {
                            connectionList.Remove(removedConnection);
                        }
                    }
                }
            }
            else
            {
                lock (_connections)
                {
                    var removedConnections = new List<TestConnection>();
                    foreach (var connection in _connections)
                    {
                        if (connection.RemoteAddress.Equals(serverAddr))
                        {
                            connection.Shutdown();
                            removedConnections.Add(connection);
                        }
                    }
                    foreach (var removedConnection in removedConnections)
                    {
                        _connections.Remove(removedConnection);
                    }
                }
            }
        }

        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            foreach (var connection in _connections)
            {
                await connection.WaitUntillAllReceivedMessageConsumed();
            }

            foreach(var connectionItm in _connectionIndex)
            {
                foreach(var connection in connectionItm.Value)
                {
                    await connection.WaitUntillAllReceivedMessageConsumed();
                }
            }
            await Task.Delay(100);
        }
    }

    /// <summary>
    /// Test implementation of INetworkServer. 
    /// </summary>
    public class TestNetworkServer : INetworkServer
    {
        private IConnectionChangeNotification _connectionNotifier;
        private TestNodeConnections _clientConnections;
        private NodeAddress _localAddr;
        private readonly TestNetworkInfr _netowrkInfr;

        public TestNetworkServer(TestNetworkInfr networkInfr)
        {
            _netowrkInfr = networkInfr;
        }

        public Task StartServer(NodeAddress serverAddr)
        {
            _localAddr = serverAddr;
            _clientConnections = new TestNodeConnections(_localAddr, _netowrkInfr);

            return Task.CompletedTask;
        }

        public Task StopServer()
        {
            return Task.CompletedTask;
        }

        public void SubscribeConnectionChangeNotification(IConnectionChangeNotification notifier)
        {
            _connectionNotifier = notifier;
        }

        /// <summary>
        /// This simulate server accept a new connection, and build the connection
        /// object for it.
        /// In production enviroment, server will receive a connection event, and 
        /// if it accept it, a new connection will be created.
        /// In test, when client want to connect the server, it will find the server
        /// via TestNetworkInfr, and call it's BuildConnection.
        /// </summary>
        /// <param name="clientAddress"></param>
        /// <returns>
        ///     true: new connection accepted.
        ///     false: new connection denied.
        /// </returns>
        public bool BuildNewConnection(NodeAddress clientAddress)
        {
            var connection = _clientConnections.AddConnection(clientAddress);
            if (connection == null)
            {
                return false;
            }
            _netowrkInfr.AddConnection(connection);
            _connectionNotifier?.OnConnectionOpened(connection);

            return true;
        }

        public NodeAddress ServerAddress => _localAddr;
    }

    /// <summary>
    /// Test network object creator
    /// </summary>
    public class TestNetworkCreator : INetworkCreator
    {
        private TestNetworkInfr _networkInfr;
        private NodeInfo _localNode;
        private int _localPort = 10240;

        public TestNetworkCreator(TestNetworkInfr networkInfr, NodeInfo localNode)
        {
            _networkInfr = networkInfr;
            _localNode = localNode;
        }

        /// <summary>
        /// Create network server, which will listen on the server address
        /// </summary>
        /// <param name="serverAddr"></param>
        /// <returns></returns>
        public async Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {

            lock(_networkInfr.NetworkServers)
            {
                foreach(var existServer in _networkInfr.NetworkServers)
                {
                    if (existServer.ServerAddress.Equals(serverAddr))
                    {
                        _networkInfr.ShutdownServerConnection(serverAddr);
                        _networkInfr.NetworkServers.Remove(existServer);
                        break;
                    }
                }
            }
            var server = new TestNetworkServer(_networkInfr);
            await server.StartServer(serverAddr);
            lock (_networkInfr.NetworkServers)
            {
                _networkInfr.NetworkServers.Add(server);
            }

            return server;
        }

        /// <summary>
        /// Create a client connection, which will connect to remote address.
        /// </summary>
        /// <param name="localAddrr"></param>
        /// <param name="serverAddr"></param>
        /// <returns></returns>
        public Task<IConnection> CreateNetworkClient(NodeAddress serverAddr)
        {
            lock(_networkInfr)
            {
                var localPort = Interlocked.Increment(ref _localPort);
                var localAddr = new NodeAddress(_localNode, localPort);
                var clientConnection = _networkInfr.GetConnection(localAddr, serverAddr);
                if (clientConnection != null)
                {
                    return Task.FromResult(clientConnection as IConnection);
                }
                clientConnection = new TestConnection(localAddr, serverAddr, _networkInfr);
                var server = _networkInfr.GetServer(serverAddr);
                if (server == null)
                {
                    return null;
                }
                if (!server.BuildNewConnection(localAddr))
                {
                    return Task.FromResult((IConnection)null);
                }
                _networkInfr.AddConnection(clientConnection);
                return Task.FromResult(clientConnection as IConnection);
            }
        }
    }

    /// <summary>
    /// Test rpc request handler, which have chance to get the rpc requests
    /// </summary>
    public class TestRpcRequestHandler : IRpcRequestHandler
    {
        private List<RpcMessage> _messageList;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        public TestRpcRequestHandler(List<RpcMessage> rpcMssageList)
        {
            _messageList = rpcMssageList;
        }

        public Task<RpcMessage> HandleRequest(RpcMessage request)
        {
            lock (_lock)
            {
                _messageList.Add(request);
            }

            return Task.FromResult((RpcMessage)null);
        }
    }
}
