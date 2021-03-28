using Microsoft.VisualStudio.TestTools.UnitTesting;
using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Message;
using PineHillRSL.Raft.Node;
using PineHillRSL.Paxos.Notebook;
using PineHillRSL.Raft.Protocol;
using PineHillRSL.Paxos.Rpc;
using PineHillRSL.Rpc;
using PineHillRSL.ReplicatedTable;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.Tests
{
    [TestClass()]
    public class RaftTests
    {
        private void InitLog()
        {

            var logFilePath = ".\\log\\logger." + DateTime.Now.Ticks.ToString() + ".log";
            var fielLogger = new FileLog(logFilePath);
            Logger.Init(fielLogger);

        }
        [TestInitialize()]
        public void TestIntialize()
        {
            int maxWorker = 0;
            int maxIocp = 0;

            ThreadPool.GetAvailableThreads(out maxWorker, out maxIocp);
            ThreadPool.GetMinThreads(out maxWorker, out maxIocp);
            var currentThreadCount = ThreadPool.ThreadCount;
            if (ThreadPool.SetMinThreads(maxWorker * 4 , maxIocp * 3))
            {
                Console.WriteLine("success increase min threads");
                ThreadPool.GetAvailableThreads(out maxWorker, out maxIocp);
                currentThreadCount = ThreadPool.ThreadCount;
            }

            InitLog();
        }

        [TestMethod()]
        public async Task LeaderVoteTest()
        {
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var nodeMap = new Dictionary<string, RaftNode>();
            foreach (var nodeAddr in cluster.Members)
            {
                var raftRole = new RaftNode(cluster, nodeAddr);
                nodeMap[ConsensusNodeHelper.GetInstanceName(nodeAddr)] = raftRole;
            }

            var raftNode1 = nodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            ConsensusDecree decree = new ConsensusDecree();
            decree.Content = "test";
            var result = await raftNode1.ProposeDecree(decree, 1);
        }
    }
}
