using Paxos.Message;
using Paxos.Network;
using Paxos.Protocol;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Tests
{
    class TestPaxosNodeTalkProxy : IPaxosNodeTalkChannel
    {
        private string _nodeName;
        private Dictionary<string, PaxosNode> _nodeMap;

        public TestPaxosNodeTalkProxy(string nodeName, Dictionary<string, PaxosNode> nodeMap)
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

    }

    class FakePaxosNodeTalker : IPaxosNodeTalkChannel
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
    }

}
