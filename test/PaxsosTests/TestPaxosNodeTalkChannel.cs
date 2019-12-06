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
    /*
    class TestNetworkTransport : INetworkTransport
    {
        private string _nodeName;
        private string _targetNode;
        private TestNetwork _network;
        private List<RpcMessage> _receivedMessages = new List<RpcMessage>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);

        public TestNetworkTransport(string nodeName, string targetNode, TestNetwork network)
        {
            _nodeName = nodeName;
            _targetNode = targetNode;
            _network = network;

            if (_network == null || string.IsNullOrEmpty(_nodeName))
            {
                throw new ArgumentNullException("Invalid TestPaxosNodeTalkProxy arguments");
            }
        }

        public Task SendMessage(RpcMessage msg)
        {
            _network.GetConnection(_targetNode, _nodeName).DeliverMessage(msg);

            return Task.CompletedTask;
        }

        public async Task<RpcMessage> ReceiveMessage()
        {
            do
            {
                await _receivedMessageSemaphore.WaitAsync();
                lock (_receivedMessages)
                {
                    if (_receivedMessages.Count > 0)
                    {
                        var message = _receivedMessages[0];
                        _receivedMessages.RemoveAt(0);
                        return message;
                    }
                }
            } while (true);
        }

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

        public void DeliverMessage(RpcMessage message)
        {
            lock (_receivedMessages)
            {
                _receivedMessages.Add(message);
                _receivedMessageSemaphore.Release();
            }
        }
    }


    class FakePaxosNodeTalker : INetworkTransport
    {
        private readonly string _nodeName;
        private FakeTestNetwork _network;
        private readonly List<RpcMessage> _messageList = new List<RpcMessage>();

        public FakePaxosNodeTalker(string nodeName, FakeTestNetwork fakeNetwork)
        {
            _nodeName = nodeName;
            _network = fakeNetwork;
        }

        public List<RpcMessage> GetNodeMessages()
        {
            return _messageList;
        }

        public void ClearMessage()
        {
            _messageList.Clear();
        }

        public Task SendMessage(RpcMessage msg)
        {

            _messageList.Add(msg);

            return Task.CompletedTask;
        }

        public Task<RpcMessage> ReceiveMessage()
        {
            var completionSource = new TaskCompletionSource<RpcMessage>();
            return completionSource.Task;
        }
    }
    */
    /*
    public class TestRpcTransport : IRpcTransport
    {
        private readonly string _nodeName;
        private FakeTestNetwork _network;
        private readonly List<PaxosMessage> _messageList = new List<PaxosMessage>();

        public TestRpcTransport(string nodeName, FakeTestNetwork network)
        {
            _nodeName = nodeName;
            _network = network;
        }

        public Task<RpcMessage> SendRequest(RpcMessage rpcRequest)
        {
            var paxosRpcMsg = PaxosRpcMessageFactory.CreatePaxosRpcMessage(rpcRequest);
            var paxosMsg = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMsg);
            _messageList.Add(paxosMsg);

            var resp = new RpcMessage()
            {
                IsRequest = false,
                RequestId = rpcRequest.RequestId
            };

            return Task.FromResult(resp);
        }

        public void RegisterRequestHandler(IRpcRequestHandler requestHandler)
        {
        }

        public List<PaxosMessage> GetNodeMessages()
        {
            return _messageList;
        }

        public void ClearMessage()
        {
            _messageList.Clear();
        }

    }
    */

    /*
    public class FakeTestNetwork
    {
        private Dictionary<string, TestRpcTransport> _transportMap = new Dictionary<string, TestRpcTransport>();

        public FakeTestNetwork()
        {

        }

        public void CreateNetworkMap(List<NodeInfo> nodeList)
        {
            foreach (var nodeInfo in nodeList)
            {
                var messageTransport = new TestRpcTransport(nodeInfo.Name, this);
                _transportMap[nodeInfo.Name] = messageTransport;
            }
        }

        public ConcurrentDictionary<string, IRpcTransport> GetRpcConnectionsFromSrc(string src)
        {
            var rpcConnections = new ConcurrentDictionary<string, IRpcTransport>();
            foreach(var target in _transportMap)
            {
                if (target.Key.Equals(src))
                {
                    continue;
                }

                rpcConnections[target.Key] = _transportMap[target.Key];
            }

            return rpcConnections;
        }

        public TestRpcTransport GetConnection(string src)
        {
            return _transportMap[src];
        }

    }
    */

    class TestConnection : IConnection
    {
        private readonly TestNetworkInfr _networkInfr;
        private readonly NodeAddress _localAddress;
        private readonly NodeAddress _remoteAddress;

        private readonly List<RpcMessage> _receivedMessages = new List<RpcMessage>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);

        public TestConnection(NodeAddress localAddr, NodeAddress remoteAddr, TestNetworkInfr networkInfr)
        {
            _localAddress = localAddr;
            _remoteAddress = remoteAddr;
            _networkInfr = networkInfr;
        }

        public Task SendMessage(RpcMessage msg)
        {
            var connection = _networkInfr.GetConnection(_remoteAddress, _localAddress);
            connection.DeliverMessage(msg);

            return Task.CompletedTask;
        }

        public async Task<RpcMessage> ReceiveMessage()
        {
            do
            {
                await _receivedMessageSemaphore.WaitAsync();
                lock (_receivedMessages)
                {
                    if (_receivedMessages.Count > 0)
                    {
                        var message = _receivedMessages[0];
                        _receivedMessages.RemoveAt(0);
                        return message;
                    }
                }
            } while (true);
        }

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

        public void DeliverMessage(RpcMessage message)
        {
            lock (_receivedMessages)
            {
                _receivedMessages.Add(message);
                _receivedMessageSemaphore.Release();
            }
        }



        public NodeAddress LocalAddress => _localAddress;

        public NodeAddress RemoteAddress => _remoteAddress;

        public List<RpcMessage> ReceivedMessages => _receivedMessages;
    }

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
        public TestConnection GetConnectionWithRemoteNode(NodeAddress remoteAddr)
        {
            TestConnection connection = null;
            if (_connections.TryGetValue(remoteAddr, out connection))
            {
                return connection;
            }
            return null;
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

    class TestNetworkInfr
    {
        List<TestConnection> _connections = new List<TestConnection>();
        List<TestNetworkServer> _servers = new List<TestNetworkServer>();

        public List<TestConnection> NetworkConnections  => _connections;

        public List<TestNetworkServer> NetworkServers => _servers;

        public TestConnection GetConnection(NodeAddress localAddr, NodeAddress remoteAddr)
        {
            if (_connections.Count == 0)
            {
                return null;
            }
            if (_connections.Where(connection =>
                connection.LocalAddress.Equals(localAddr) &&
                connection.RemoteAddress.Equals(remoteAddr)).Count() == 0)
            {
                return null;
            }
           return _connections.Where(connection =>
                connection.LocalAddress.Equals(localAddr) &&
                connection.RemoteAddress.Equals(remoteAddr)).First();
        }

        public TestNetworkServer GetServer(NodeAddress serverAddr)
        {
            if (_servers.Where(server =>
                server.ServerAddress.Equals(serverAddr)).Count() == 0)
            {
                return null;
            }
            return _servers.Where(server =>
                server.ServerAddress.Equals(serverAddr)).First();
        }

    }


    class TestNetwork
    {
        TestNetworkInfr _netorkInfr;
        public TestNetwork(TestNetworkInfr networkInfr)
        {
            _netorkInfr = networkInfr;
        }

        public void CreateNetworkMap(List<NodeInfo> nodeList)
        {
            /*
            foreach (var nodeInfo in nodeList)
            {
                var server = new TestNetworkServer(_netorkInfr);
                server.StartServer(new NodeAddress()
                {
                    Node = nodeInfo,
                    Port = 0
                });
                _netorkInfr.NetworkServers.Add(server);
            }

            foreach(var nodeInfo in nodeList)
            {
                foreach (var targetNodeInfo in nodeList)
                {
                    if (nodeInfo.Equals(targetNodeInfo))
                    {
                        continue;
                    }
                    var localAddr = new NodeAddress()
                    { Node = nodeInfo, Port = 0 };
                    var remoteAddr = new NodeAddress()
                    { Node = targetNodeInfo, Port = 0 };

                    var clientConnection = new TestConnection(
                        localAddr, remoteAddr, _netorkInfr);
                    _netorkInfr.NetworkConnections.Add(clientConnection);
                    var server = _netorkInfr.GetServer(remoteAddr);
                    server.BuildNewConnection(localAddr);
                }
            }*/
        }

        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            foreach (var connection in _netorkInfr.NetworkConnections)
            {
                await connection.WaitUntillAllReceivedMessageConsumed();
            }

        }
    }

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

        public void SubscribeConnectionChangeNotification(IConnectionChangeNotification notifier)
        {
            _connectionNotifier = notifier;
        }

        public bool BuildNewConnection(NodeAddress clientAddress)
        {
            var connection = _clientConnections.AddConnection(clientAddress);
            if (connection == null)
            {
                return false;
            }
            _netowrkInfr.NetworkConnections.Add(connection);
            _connectionNotifier?.OnNewConnection(connection);

            return true;
        }

        public NodeAddress ServerAddress => _localAddr;
    }




    class TestNetworkCreator : INetworkCreator
    {
        private TestNetworkInfr _networkInfr;

        public TestNetworkCreator(TestNetworkInfr networkInfr)
        {
            _networkInfr = networkInfr;
        }

        public async Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {
            var server = new TestNetworkServer(_networkInfr);
            await server.StartServer(serverAddr);

            _networkInfr.NetworkServers.Add(server);
            return server;
        }

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
                _networkInfr.NetworkConnections.Add(clientConnection);
                return Task.FromResult(clientConnection as IConnection);
            }
        }
    }

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
