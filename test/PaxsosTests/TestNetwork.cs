using Paxos.Common;
using Paxos.Message;
using Paxos.Network;
using Paxos.Rpc;
using Paxos.PerCounter;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace Paxos.Tests
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
        //private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private List<int> _lock = new List<int>();

        private Random _random = new Random();

        public TestConnection(NodeAddress localAddr, NodeAddress remoteAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _remoteAddress = remoteAddr;
            _networkInfr = networkInfr;
        }

        public async Task SendMessage(NetworkMessage msg)
        {
            using (var sendMsgTimer = new PerfTimerCounter((int)TestPerfCounterType.SendMessageTime))
            {
                if (_remoteConnection == null)
                {
                    _remoteConnection = _networkInfr.GetConnection(_remoteAddress, _localAddress);

                }
                var begin = DateTime.Now;
                var delayMs = _random.Next(3, 5);
                await Task.Delay(delayMs);
                var delayedTime = DateTime.Now - begin;
                _remoteConnection.DeliverMessage(msg);
                var sentTime = DateTime.Now - begin;
                if (sentTime.TotalMilliseconds > 200)
                {
                    //Console.WriteLine("send cost too much time");
                }
            }
        }

        public async Task<List<NetworkMessage>> ReceiveMessage()
        {
            var newMsgList = new List<NetworkMessage>();
            List<NetworkMessage> result = null;
            do
            {
                await _receivedMessageSemaphore.WaitAsync();
                lock (_lock)
                {
                    if (_receivedMessages.Count > 0)
                    {
                        //var message = _receivedMessages[0];
                        //_receivedMessages.RemoveAt(0);
                        // return message;
                         result = _receivedMessages;
                        _receivedMessages = newMsgList;
                        break;
                    }
                }
            } while (true);

            var now = DateTime.Now;
            foreach (var msg in result)
            {
                msg.ReceivedTime = now;
            }
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
            lock (_lock)
            {
                message.DeliveredTime = DateTime.Now;
                _receivedMessages.Add(message);
            }
            _receivedMessageSemaphore.Release();
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
        ConcurrentDictionary<int, TestConnection> _connectionIndex = new ConcurrentDictionary<int, TestConnection>();

        //public List<TestConnection> NetworkConnections  => _connections;
        private bool _useConnectionMap = true;
        public void AddConnection(TestConnection connection)
        {
            if (_useConnectionMap)
            {
                _connectionIndex.TryAdd(connection.LocalAddress.GetHashCode() + connection.RemoteAddress.GetHashCode() * 10, connection);
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
                    var key = localAddr.GetHashCode() + remoteAddr.GetHashCode() * 10;
                    TestConnection connection = null;
                    if (!_connectionIndex.TryGetValue(key, out connection))
                    {
                        return null;
                    }
                    if (connection == null)
                    {
                        return null;
                    }
                    else
                    {
                        if (connection.LocalAddress.Equals(localAddr) &&
                            connection.RemoteAddress.Equals(remoteAddr))
                        {
                            return connection;
                        }
                        return null;
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

        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            foreach (var connection in _connections)
            {
                await connection.WaitUntillAllReceivedMessageConsumed();
            }
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
            _connectionNotifier?.OnNewConnection(connection);

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

        public TestNetworkCreator(TestNetworkInfr networkInfr)
        {
            _networkInfr = networkInfr;
        }

        /// <summary>
        /// Create network server, which will listen on the server address
        /// </summary>
        /// <param name="serverAddr"></param>
        /// <returns></returns>
        public async Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {
            var server = new TestNetworkServer(_networkInfr);
            await server.StartServer(serverAddr);

            lock(_networkInfr.NetworkServers)
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
        public Task<IConnection> CreateNetworkClient(NodeAddress localAddrr, NodeAddress serverAddr)
        {
            //lock(_networkInfr)
            {
                var clientConnection = _networkInfr.GetConnection(localAddrr, serverAddr);
                if (clientConnection != null)
                {
                    return Task.FromResult(clientConnection as IConnection);
                }
                clientConnection = new TestConnection(localAddrr, serverAddr, _networkInfr);
                var server = _networkInfr.GetServer(serverAddr);
                if (!server.BuildNewConnection(localAddrr))
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
