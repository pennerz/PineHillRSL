using System;
using PaxosLib;
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
                throw new ArgumentNullException("Invalid TestPaxosNodeTalkProxy argements");
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
}
