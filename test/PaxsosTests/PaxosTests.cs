using Microsoft.VisualStudio.TestTools.UnitTesting;
using PaxosLib;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Tests
{
    [TestClass()]
    public class PaxosTests
    {
        [TestMethod()]
        public async Task BeginNewProposeTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo();
                node.Name = "Node" + i.ToString();
                cluster.Members.Add(node);
            }

            Dictionary<string, PaxosNode> nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeInfo in cluster.Members)
            {
                var proxy = new TestPaxosNodeTalkProxy(nodeInfo.Name, nodeMap);
                var node = new PaxosNode(proxy, cluster, nodeInfo);
                nodeMap[nodeInfo.Name] = node;
            }

            var properser = nodeMap[cluster.Members[0].Name];

            var decree = new PaxosDecree()
            {
                Content = "test"
            };
            var result = await properser.ProposeDecree(decree, 0);
            var properser2 = nodeMap[cluster.Members[1].Name];

            var decree2 = new PaxosDecree()
            {
                Content = "test2"
            };
            var result1 = await properser2.ProposeDecree(decree2, result.DecreeNo);

        }
    }
}

namespace PaxsosTests
{
    class PaxosTests
    {
    }
}
