using Paxos.Message;
using Paxos.Network;
using Paxos.Protocol;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Paxos.Tests
{
    class TestMessageTransport : IMessageTransport
    {
        private string _nodeName;
        private Dictionary<string, TestMessageTransport> _nodeMap;
        private List<PaxosMessage> _receivedMessages = new List<PaxosMessage>();
        private SemaphoreSlim _receivedMessageSemaphore = new SemaphoreSlim(0);

        public TestMessageTransport(string nodeName, Dictionary<string, TestMessageTransport> nodeMap)
        {
            _nodeName = nodeName;
            _nodeMap = nodeMap;

            if (_nodeMap == null || string.IsNullOrEmpty(_nodeName))
            {
                throw new ArgumentNullException("Invalid TestPaxosNodeTalkProxy arguments");
            }
        }

        public Task SendMessage(PaxosMessage msg)
        {
            msg.SourceNode = _nodeName;
            var targetNode = msg.TargetNode;

            _nodeMap[targetNode]?.DeliverMessage(msg);

            return Task.CompletedTask;
        }

        public async Task<PaxosMessage> ReceiveMessage()
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

        public void DeliverMessage(PaxosMessage message)
        {
            lock(_receivedMessages)
            {
                _receivedMessages.Add(message);
                _receivedMessageSemaphore.Release();
            }
        }
    }

    class FakePaxosNodeTalker : IMessageTransport
    {
        private readonly string _nodeName;
        private readonly Dictionary<string, List<PaxosMessage>> _messageList = new Dictionary<string, List<PaxosMessage>>();

        public FakePaxosNodeTalker(string nodeName)
        {
            _nodeName = nodeName;
        }

        public List<PaxosMessage> GetNodeMessages(string nodeName)
        {
            if (string.IsNullOrEmpty(nodeName))
            {
                return null;
            }

            if (_messageList.ContainsKey(nodeName))
            {
                return _messageList[nodeName];
            }
            return null;
        }

        public void ClearNodeMessage(string nodeName)
        {
            if (string.IsNullOrEmpty(nodeName))
            {
                return;
            }

            if (_messageList.ContainsKey(nodeName))
            {
                _messageList[nodeName].Clear();
            }
        }

        public void BuildNodeMessageList(string nodeName)
        {
            if (!_messageList.ContainsKey(nodeName))
            {
                _messageList.Add(nodeName, new List<PaxosMessage>());
            }
        }

        public Task SendMessage(PaxosMessage msg)
        {
            msg.SourceNode = _nodeName;
            var targetNode = msg.TargetNode;

            if (!_messageList.ContainsKey(targetNode))
            {
                _messageList.Add(targetNode, new List<PaxosMessage>());
            }
            _messageList[targetNode].Add(msg);

            return Task.CompletedTask;
        }

        public Task<PaxosMessage> ReceiveMessage()
        {
            return Task.FromResult((PaxosMessage)null);
        }
    }
}
