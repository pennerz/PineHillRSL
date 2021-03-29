using Microsoft.VisualStudio.TestTools.UnitTesting;
using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Raft.Message;
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
        [TestMethod()]
        public async Task FollowerVoteReqTest()
        {
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }

            var leaderNode = cluster.Members[0];
            var followerNode = cluster.Members[1];

            //
            // 1. vote req's current term < follower's current term, deny
            // 2. vote req's current term < follower's candidate term, deny
            // 3. vote req's last log entry's < follower's committed log entry, deny
            // 4. otherwise, accept
            //

            // 1. vote request's term < current term, deny
            var follower = new RaftFollower();
            follower.TestSetCurrentTerm(4);
            follower.TestSetLogIndex(12);   // crrent term, 4, log index 12
            follower.TestSetCandidateTerm(4, leaderNode); // candidate term 4
            follower.TestSetCommittedLogIndex(11); // committed logIndex, 4:11 

            var voteReq = new VoteReqMessage();
            voteReq.SourceNode = NodeAddress.Serialize(leaderNode);
            voteReq.TargetNode = NodeAddress.Serialize(followerNode);
            voteReq.Term = 2;
            voteReq.LogIndex = 10; // no care
            voteReq.LastLogEntryTerm = 1;
            voteReq.LastLogEntryIndex = 10;

            var voteResp = await follower.DoRequest(voteReq) as VoteRespMessage;
            Assert.IsFalse(voteResp.Succeed);
            Assert.IsTrue(follower.GetLeader() == null);

            // 2. vote request's term < foller's candidate term, deny
            follower.TestSetCurrentTerm(1);
            follower.TestSetCandidateTerm(2, leaderNode); // candidate term 2
            follower.TestSetLogIndex(12);   // crrent term, 1, log index 12
            follower.TestSetCommittedLogIndex(11); // committed logIndex, 1:11 
            voteReq.SourceNode = NodeAddress.Serialize(leaderNode);
            voteReq.TargetNode = NodeAddress.Serialize(followerNode);
            voteReq.Term = 2;
            voteReq.LogIndex = 10; // no care
            voteReq.LastLogEntryTerm = 1;
            voteReq.LastLogEntryIndex = 10;
            voteResp = await follower.DoRequest(voteReq) as VoteRespMessage;
            Assert.IsFalse(voteResp.Succeed);

            // 3. vote req's last log entry's < follower's committed log entry, deny
            follower.TestSetCurrentTerm(1);
            follower.TestSetCandidateTerm(2, leaderNode); // candidate term 2
            follower.TestSetLogIndex(12);   // crrent term, 1, log index 12
            follower.TestSetCommittedLogIndex(11); // committed logIndex, 1:11 

            voteReq.SourceNode = NodeAddress.Serialize(leaderNode);
            voteReq.TargetNode = NodeAddress.Serialize(followerNode);
            voteReq.Term = 3;
            voteReq.LogIndex = 10; // no care
            voteReq.LastLogEntryTerm = 1;
            voteReq.LastLogEntryIndex = 10;
            voteResp = await follower.DoRequest(voteReq) as VoteRespMessage;
            Assert.IsFalse(voteResp.Succeed); // lastlogentry: (1,10) < committedlogentry: (1, 11)

            // 4. req's term > current term, req's term > candidate term,
            //    req's lastlogentry's term, log index >= committedlogentry's term, index
            follower.TestSetCurrentTerm(1);
            follower.TestSetCandidateTerm(2, leaderNode); // candidate term 2
            follower.TestSetLogIndex(12);   // crrent term, 1, log index 12
            follower.TestSetCommittedLogIndex(11); // committed logIndex, 1:11 
            voteReq.Term = 3;
            voteReq.LogIndex = 11; // no care
            voteReq.LastLogEntryTerm = 1;
            voteReq.LastLogEntryIndex = 11;
            voteResp = await follower.DoRequest(voteReq) as VoteRespMessage;
            Assert.IsTrue(voteResp.Succeed); // lastlogentry: (1,11) >= committedlogentry: (1, 11)
            Assert.AreEqual(follower.CandidateTerm, (UInt64)3);
            Assert.IsTrue(leaderNode.Equals(follower.CandidateNode));
            Assert.IsTrue(follower.GetLeader() == null);
        }

        [TestMethod()]
        public async Task FollowerAppendReqTest()
        {
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }


            var leaderNode = cluster.Members[0];
            var followerNode = cluster.Members[1];
            //
            // 1. append req's current term < follower's current term, deny
            // 2. otherwise, accpet
            //

            // append request
            var follower = new RaftFollower();
            follower.TestSetCurrentTerm(2);
            follower.TestSetCandidateTerm(3, leaderNode); // candidate term 3
            follower.TestSetLogIndex(12);   // crrent term, 2, log index 12
            follower.TestSetCommittedLogIndex(11); // committed logIndex, 2:11 

            // 1. append req's current term < follower's current term, deny
            var appendReq = new AppendEntityReqMessage();
            appendReq.SourceNode = NodeAddress.Serialize(leaderNode);
            appendReq.TargetNode = NodeAddress.Serialize(followerNode);
            appendReq.Term = 1;
            appendReq.LogIndex = 12;
            appendReq.CommittedLogIndex = 11;
            var appendResp = await follower.DoRequest(appendReq) as AppendEntityRespMessage;
            Assert.IsFalse(appendResp.Succeed);
            Assert.IsTrue(follower.GetLeader() == null);

            // 2. append req's current term >= foller's current term
            appendReq.Term = 3;
            appendResp = await follower.DoRequest(appendReq) as AppendEntityRespMessage;
            Assert.IsTrue(appendResp.Succeed);
            Assert.IsTrue(follower.GetLeader().Equals(leaderNode));

            // 3. append request's term >= foller's current term,
            // but committed log entry index <= folloer's committed log index
            // Impossible, follower return false
            appendReq.CommittedLogIndex = 10;
            appendReq.LogIndex = 12;
            appendResp = await follower.DoRequest(appendReq) as AppendEntityRespMessage;
            Assert.IsFalse(appendResp.Succeed);

        }
    }
}
