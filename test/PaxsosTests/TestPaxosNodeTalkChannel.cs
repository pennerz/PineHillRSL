using Paxos.Message;
using Paxos.Network;
using Paxos.Protocol;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Tests
{
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

    class TestNetwork
    {
        private Dictionary<Tuple<string, string>, TestNetworkTransport>  _transportMap = new Dictionary<Tuple<string, string>, TestNetworkTransport>();

        public TestNetwork()
        {

        }

        public void CreateNetworkMap(List<NodeInfo> nodeList)
        {
            foreach (var nodeInfo in nodeList)
            {
                foreach (var targetNodeInfo in nodeList)
                {
                    if (nodeInfo.Equals(targetNodeInfo))
                    {
                        continue;
                    }

                    var messageTransport = new TestNetworkTransport(nodeInfo.Name, targetNodeInfo.Name, this);
                    var key = new Tuple<string, string>(nodeInfo.Name, targetNodeInfo.Name);
                    _transportMap[key] = messageTransport;
                }
            }
        }

        public ConcurrentDictionary<string, IRpcTransport> GetRpcConnectionsFromSrc(string src)
        {
            var rpcConnections = new ConcurrentDictionary<string, IRpcTransport>();
            foreach (var connection in _transportMap)
            {
                if (connection.Key.Item1.Equals(src))
                {
                    rpcConnections[connection.Key.Item2] = new RpcTransport(connection.Value);

                }
            }

            return rpcConnections;
        }

        public TestNetworkTransport GetConnection(string src, string target)
        {
            return _transportMap[new Tuple<string, string>(src, target)];
        }

        public async Task WaitUntillAllReceivedMessageConsumed()
        {
            foreach(var connection in _transportMap)
            {
                await connection.Value.WaitUntillAllReceivedMessageConsumed();
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

}
