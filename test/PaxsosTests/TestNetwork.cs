using Paxos.Message;
using Paxos.Network;
using Paxos.Rpc;
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
    class TestConnection : IConnection
    {
        private readonly TestNetworkInfr _networkInfr;
        private readonly NodeAddress _localAddress;
        private readonly NodeAddress _remoteAddress;

        private TestConnection _remoteConnection = null;

        private List<NetworkMessage> _receivedMessages = new List<NetworkMessage>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);

        public TestConnection(NodeAddress localAddr, NodeAddress remoteAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _remoteAddress = remoteAddr;
            _networkInfr = networkInfr;
        }

        public Task SendMessage(NetworkMessage msg)
        {
            if (_remoteConnection == null)
            {
                _remoteConnection = _networkInfr.GetConnection(_remoteAddress, _localAddress);

            }
            _remoteConnection.DeliverMessage(msg);

            return Task.CompletedTask;
        }

        public async Task<List<NetworkMessage>> ReceiveMessage()
        {
            do
            {
                await _receivedMessageSemaphore.WaitAsync();
                var newMsgList = new List<NetworkMessage>();
                lock (_receivedMessages)
                {
                    if (_receivedMessages.Count > 0)
                    {
                        //var message = _receivedMessages[0];
                        //_receivedMessages.RemoveAt(0);
                        // return message;

                        var result = _receivedMessages;
                        _receivedMessages = newMsgList;
                        return result;
                    }
                }
            } while (true);
        }

        /// <summary>
        /// Wait for all received message to be consumed.
        /// </summary>
        /// <returns></returns>
        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            do
            {
                lock (_receivedMessages)
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
            lock (_receivedMessages)
            {
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
    class TestNodeConnections
    {
        private NodeAddress _localAddress;
        private Dictionary<NodeAddress, TestConnection> _connections =
            new Dictionary<NodeAddress, TestConnection>();
        private TestNetworkInfr _networkInfr = null;

        public TestNodeConnections(NodeAddress localAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _networkInfr = networkInfr;
        }

        public TestConnection AddConnection(NodeAddress remoteAddress)
        {
            lock(_connections)
            {
                if (!_connections.ContainsKey(remoteAddress))
                {
                    var connection = new TestConnection(
                        _localAddress, remoteAddress, _networkInfr);

                    _connections.Add(remoteAddress, connection);
                    return connection;
                }
                return _connections[remoteAddress];
            }
        }
    }

    /// <summary>
    /// Network map which include all the nodes' connections. Test connection
    /// rely on it to find the right connection to deliver the message.
    /// </summary>
    class TestNetworkInfr
    {
        List<TestConnection> _connections = new List<TestConnection>();
        List<TestNetworkServer> _servers = new List<TestNetworkServer>();
        Dictionary<int, List<TestConnection>> _connectionIndex = new Dictionary<int, List<TestConnection>>();

        //public List<TestConnection> NetworkConnections  => _connections;
        private bool _useConnectionMap = false;
        public void AddConnection(TestConnection connection)
        {
            lock(_connections)
            {
                if (_useConnectionMap)
                {
                    _connectionIndex.TryAdd(connection.LocalAddress.GetHashCode() + connection.RemoteAddress.GetHashCode(), new List<TestConnection>());
                    _connectionIndex[connection.LocalAddress.GetHashCode() + connection.RemoteAddress.GetHashCode()].Add(connection);
                }
                else
                {
                    _connections.Add(connection);
                }
            }
        }

        public List<TestNetworkServer> NetworkServers => _servers;

        public TestConnection GetConnection(NodeAddress localAddr, NodeAddress remoteAddr)
        {
            lock(_connections)
            {
                if (_useConnectionMap)
                {
                    var key = localAddr.GetHashCode() + remoteAddr.GetHashCode();
                    if (!_connectionIndex.ContainsKey(key))
                    {
                        return null;
                    }
                    var connectionList = _connectionIndex[key];
                    if (connectionList.Count == 1)
                    {
                        return connectionList[0];
                    }
                    else if (connectionList.Count == 0)
                    {
                        return null;
                    }
                    else
                    {
                        var matchList = connectionList.Where(connection =>
                           connection.LocalAddress.Equals(localAddr) &&
                           connection.RemoteAddress.Equals(remoteAddr));

                        if (matchList.Count() == 0)
                        {
                            return null;
                        }
                        return matchList.First();
                    }
                }
                else
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
    class TestNetworkServer : INetworkServer
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
    class TestNetworkCreator : INetworkCreator
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
            lock(_networkInfr)
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
        public TestRpcRequestHandler(List<RpcMessage> rpcMssageList)
        {
            _messageList = rpcMssageList;
        }

        public Task<RpcMessage> HandleRequest(RpcMessage request)
        {
            _messageList.Add(request);

            return Task.FromResult((RpcMessage)null);
        }
    }
}
