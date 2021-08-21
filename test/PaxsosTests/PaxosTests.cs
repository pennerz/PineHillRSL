using Microsoft.VisualStudio.TestTools.UnitTesting;
using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Message;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Paxos.Notebook;
using PineHillRSL.Paxos.Protocol;
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
    public class PaxosTests
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
        public async Task BeginNewProposeTest()
        {
            await ProposeTest(true);
            await ProposeTest(false);
        }

        private async Task ProposeTest(bool notifyLearner)
        {
            CleanupLogFiles(null);

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeAddr);
                nodeMap[ConsensusNodeHelper.GetInstanceName(nodeAddr)] = node;
            }

            var proposer = nodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            proposer.NotifyLearner = notifyLearner;
            var decree = new ConsensusDecree()
            {
                Content = "test"
            };
            var result = await proposer.ProposeDecree(decree, 0/*nextDecreNo*/);
            var readReslut = await proposer.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 1);

            var proposer2 = nodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[1])];
            proposer2.NotifyLearner = notifyLearner;
            var decree2 = new ConsensusDecree()
            {
                Content = "test2"
            };
            // this will get the committed decree in proposer1
            result = await proposer2.ProposeDecree(decree2, result.DecreeNo);
            Assert.IsTrue(result.Decree.Content.Equals("test"));
            Assert.IsTrue(result.DecreeNo == 1);

            readReslut = await proposer2.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 1);

            // proposer1 propose a new decree
            result = await proposer.ProposeDecree(decree2, 0);
            Assert.IsTrue(result.Decree.Content.Equals("test2"));
            Assert.IsTrue(result.DecreeNo == 2);
            readReslut = await proposer.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test2"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 2);

            // proposer2 propose a new decree
            var decree3 = new ConsensusDecree()
            {
                Content = "test3"
            };
            result = await proposer2.ProposeDecree(decree3, 0);
            Assert.IsTrue(result.Decree.Content.Equals("test3"));
            Assert.IsTrue(result.DecreeNo == 3);
            await networkInfr.WaitUntillAllReceivedMessageConsumed();
            await Task.Delay(50); // message received but may not processed
            readReslut = await proposer.ReadDecree(result.DecreeNo);
            if (notifyLearner)
            {
                Assert.IsTrue(readReslut.IsFound);
                Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
                Assert.IsTrue(readReslut.MaxDecreeNo == 3);
            }
            else
            {
                Assert.IsFalse(readReslut.IsFound);
                Assert.IsTrue(readReslut.MaxDecreeNo == 2);
                await proposer.ProposeDecree(new ConsensusDecree("anythingelse"), 3);
                readReslut = await proposer.ReadDecree(3);
                Assert.IsTrue(readReslut.IsFound);
                Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
                Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            }

            readReslut = await proposer2.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            foreach (var node in nodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }

        [TestMethod()]
        public async Task PaxosNodeBootstrapTest()
        {
            var cluster = new ConsensusCluster();
            CleanupLogFiles(null);

            await BeginNewProposeTest();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeAddr);
                var instanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);
                nodeMap[instanceName] = node;
                var metaLogFile = ".\\storage\\" + instanceName + ".meta";
                await node.Load(metaLogFile);
            }

            var proposer = nodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];

            var readReslut = await proposer.ReadDecree(1);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            var proposer2 = nodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[1])];

            // this will get the committed decree in proposer1
            readReslut = await proposer2.ReadDecree(1);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            // proposer1 propose a new decree
            readReslut = await proposer.ReadDecree(2);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test2"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            // proposer2 propose a new decree
            readReslut = await proposer.ReadDecree(3);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            foreach (var node in nodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }

        [TestMethod()]
        public async Task VoteRoleTest()
        {
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 108 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var sourceNode = NodeAddress.Serialize(cluster.Members[0]);
            var targetNode = NodeAddress.Serialize(cluster.Members[1]);
            var srcServerAddress = cluster.Members[0];
            //var srcClientAddress = new NodeAddress(new NodeInfo(sourceNode), 0);
            var targetServerAddress = cluster.Members[1];
            //var targetClientAddress = new NodeAddress(new NodeInfo(targetNode), 0);

            var logPrefix = Guid.NewGuid().ToString();

            var ledgerLogger = new FileLogger(".\\storage\\" + logPrefix + "logger_node1.log");
            var votedLogger = new FileLogger(".\\storage\\" + logPrefix + "votedlogger_node1.log");
            var proposerNote = new ProposerNote(ledgerLogger);
            var voterNote = new VoterNote(votedLogger);

            var msgList = new List<RpcMessage>();
            // 1. NextBallotMessage
            {
                // 1.1. voter have voted no ballot for a decree
                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                var rpcClient = new RpcClient();
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, voterNote, proposerNote);
                voter.WaitRpcResp = true;
                var nextBallotMsg = new NextBallotMessage();
                nextBallotMsg.DecreeNo = 1;
                nextBallotMsg.BallotNo = 1;
                nextBallotMsg.SourceNode = sourceNode;
                nextBallotMsg.TargetNode = targetNode;
                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                var connection = await rpcClient.GetConnection(srcServerAddress);

                var propserConnection = networkInfr.GetConnection(srcServerAddress, connection.RemoteAddress);
                Assert.AreEqual(msgList.Count, 1);
                var lastVoteMsg = CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.AreEqual(lastVoteMsg.VoteDecreeContent, null);
                Assert.AreEqual(voterNote.GetNextBallotNo(nextBallotMsg.DecreeNo), (ulong)1);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                // now the NextBalloNo is 1
                // 1.2. New ballot no > 1 will be accepted and got a last vote
                nextBallotMsg.BallotNo = 2;
                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.AreEqual(lastVoteMsg.VoteDecreeContent, null);
                Assert.AreEqual(voterNote.GetNextBallotNo(nextBallotMsg.DecreeNo), (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                // now NextBallotNo is 2

                // 1.3. NextBallotNo <= 2 will not be accepted and got a stale ballot message
                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg =  CreatePaxosMessage(msgList[0]) as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)2);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                nextBallotMsg.BallotNo = 1;
                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                staleBallotMsg =  CreatePaxosMessage(msgList[0]) as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                //1.4 voter has voted ballot before
                string voteContent = "test1";
                var voteMsg = new VoteMessage();
                voteMsg.DecreeNo = 1;
                voteMsg.BallotNo = 2;
                voteMsg.SourceNode = sourceNode;
                voteMsg.TargetNode = targetNode;
                await voterNote.UpdateLastVote(1, 2,
                    new ConsensusDecree(){ Content = voteContent });

                nextBallotMsg.BallotNo = 3;
                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)3);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecreeContent, voteContent);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                //1.5 decree has been committed in ledger
                await proposerNote.CommitDecree(1, new ConsensusDecree(lastVoteMsg.VoteDecreeContent));
                nextBallotMsg.BallotNo = 2; // do not care about the ballot no for committed decree

                await voter.HandlePaxosMessage(nextBallotMsg);
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                //Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecreeContent, voteContent);
                Assert.IsTrue(lastVoteMsg.Commited);
                msgList.Clear();

                // cleanup
                await proposerNote.Clear();

            }

            // 2. StartNewBallotMessage
            {
                voterNote.Reset();

                networkInfr = new TestNetworkInfr();
                NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                // 2.2 has no NextBallotNo yet
                var rpcClient = new RpcClient();
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, voterNote, proposerNote);
                voter.WaitRpcResp = true;

                var connection = await rpcClient.GetConnection(srcServerAddress);
                var propserConnection = networkInfr.GetConnection(srcServerAddress, connection.LocalAddress);

                string voteContent = "test1";
                var beginBallotMsg = new BeginBallotMessage();
                beginBallotMsg.DecreeNo = 1;
                beginBallotMsg.BallotNo = 1;
                beginBallotMsg.SourceNode = sourceNode;
                beginBallotMsg.TargetNode = targetNode;
                beginBallotMsg.DecreeContent = voteContent;
                await voter.HandlePaxosMessage(beginBallotMsg);    // no response
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 0);

                await voterNote.UpdateNextBallotNo(1, 2); // nextBallotNo = 2, > 1
                await voter.HandlePaxosMessage(beginBallotMsg);    // state ballot response
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg =  CreatePaxosMessage(msgList[0]) as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);    //nextBallotNo > ballotNo 
                msgList.Clear();


                // 2.2 NextBallotNo match
                beginBallotMsg.BallotNo = 2;
                await voter.HandlePaxosMessage(beginBallotMsg);    // vote
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                var voteMsg =  CreatePaxosMessage(msgList[0]) as VoteMessage;
                Assert.IsNotNull(voteMsg);
                Assert.AreEqual(voteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(voteMsg.BallotNo, (ulong)2);

                msgList.Clear();

                // 2.3 Decree committed, return a lastvotemsg, indicate the decree committed
                await proposerNote.CommitDecree(beginBallotMsg.DecreeNo, new ConsensusDecree(beginBallotMsg.Decree));
                beginBallotMsg.BallotNo = 3;
                await voter.HandlePaxosMessage(beginBallotMsg);    // vote
                await voter.WaitForAllMessageSent();
                await networkInfr.WaitUntillAllReceivedMessageConsumed();

                Assert.AreEqual(msgList.Count, 1);
                var lastVoteMsg = CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)3);
                Assert.IsTrue(lastVoteMsg.Commited);

                msgList.Clear();
            }

            // 3 Commit
            {

            }

            await voterNote.DisposeAsync();
            await proposerNote.DisposeAsync();
            await ledgerLogger.DisposeAsync();
            await votedLogger.DisposeAsync();
        }

        [TestMethod()]
        public async Task ProposerRoleStateMachineTest()
        {
            // intialize

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 118 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            var srcServerAddress = cluster.Members[0];
            var targetServerAddress = cluster.Members[1];

            List<List<RpcMessage>> msgList = new List<List<RpcMessage>>();
            List<RpcServer> serverList = new List<RpcServer>();
            foreach (var nodeAddr in cluster.Members)
            {
                var svrAddr = nodeAddr;
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }


            var logPrefix = Guid.NewGuid().ToString();
            var ledgerLogger = new FileLogger(".\\storage\\" + logPrefix + "logger.log");
            var proposerNote = new ProposerNote(ledgerLogger);
            var proposeManager = new ProposeManager(await proposerNote.GetMaximumCommittedDecreeNo() + 1);

            var rpcClient = new RpcClient();


            // state transition map
            //
            //
            //                    stale message                                   commited returned
            //                   |--------------|    |--------------------------------------------------------------------------------
            //                  \|/             |   |                                                                              \|/
            //         collect_last_vote -> wait_for_last_vote -> beginnewballot -> wait_for_new_ballot_result ->ReadyCommit -->  committed
            //            /|\  /|\                   |                                    | |                                       /|\
            //             |    ---------------------|                                    | |----------------------------------------|
            //             |              timeout                                         |     others has new propose and committed
            //             |                                                              |
            //             |______________________________________________________________|
            //              stale message indicate ballot already occupied by new proposer

            // 2. collect_last_vote -> wait_for_last_vote
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);

                proposeManager.Reset();
            }

            // 3. wait_for_last_vote -> beginnewballot (enough last vote msg received)
            // 3.1 none return with voted decree
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 2;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = nextBallotNo - 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);

                // last vote message(with null vote) will not change state
                for (int i = 0; i < 5; i++)
                {
                    var lastVote = CreateLastVoteMessage(
                        NodeAddress.Serialize(cluster.Members[i]),
                        NodeAddress.Serialize(cluster.Members[0]),
                        nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/,
                        0/*votedBallotNo*/, null/*votedDecree*/);
                    await proposer.HandlePaxosMessage(lastVote);
                    if (i >= 2)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        Assert.IsTrue(propose.LastVoteMessages.Count >= 3);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, i + 1);
                        var returnedLastVote = propose.LastVoteMessages[i];
                        VerifyLastVoteMessage(returnedLastVote, nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/, 0/*votedBallotNo*/, null/*votedContent*/);
                    }

                    if (i >= 2)
                    {
                        VerifyPropose(propose, nextBallotNo, ProposeState.BeginNewBallot, decreeContent);
                    }
                    else
                    {
                        VerifyPropose(propose, nextBallotNo, ProposeState.WaitForLastVote, decreeContent);
                    }
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                var collectLastVoteResult = await collectLastVoteTask;

                proposeManager.Reset();
            }

            // 3.2 more than one returned with voted decree, the decree with higher ballot no will be treated with voted decree
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 3;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = nextBallotNo - 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);

                // last vote message
                string decreeContent1 = "test2";
                for (int i = 0; i < 5; i++)
                {
                    LastVoteMessage lastVote = null;
                    if (i == 2)
                    {
                        lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/,
                            3/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                    }
                    else if (i == 1)
                    {
                        lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/,
                            2/*votedBallotNo*/, decreeContent/*votedDecree*/);
                    }
                    else
                    {
                        lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/,
                            0/*votedBallotNo*/, null/*votedDecree*/);
                    }

                    await proposer.HandlePaxosMessage(lastVote);
                    if (i >= 2)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, 3);
                        var returnedLastVote = propose.LastVoteMessages[2]; // only 3 accepeted
                        VerifyLastVoteMessage(returnedLastVote, nextDecreeNo, nextBallotNo, 3/*votedBallotNo*/, decreeContent1/*votedContent*/);
                        VerifyPropose(propose, nextBallotNo/*lastTriedBallot*/, ProposeState.BeginNewBallot, decreeContent1);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, i + 1);
                        if (i == 1)
                        {
                            var returnedLastVote = propose.LastVoteMessages[i];
                            VerifyLastVoteMessage(returnedLastVote, nextDecreeNo, nextBallotNo, 2/*votedBallotNo*/, decreeContent/*votedContent*/);
                        }
                        else if (i == 0)
                        {
                            var returnedLastVote = propose.LastVoteMessages[i];
                            VerifyLastVoteMessage(returnedLastVote, nextDecreeNo, nextBallotNo, 0/*votedBallotNo*/, null/*votedContent*/);
                        }
                        VerifyPropose(propose, nextBallotNo, ProposeState.WaitForLastVote, decreeContent);
                    }
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                var collectLastVoteResult = await collectLastVoteTask;

                proposeManager.Reset();
            }

            // 4. wait_for_last_vote -> collect_last_vote (timeout)
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 3;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = nextBallotNo - 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);
                await Task.Delay(3000); // now time out is 2s
                var collectLastVoteResult = await collectLastVoteTask;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                proposeManager.Reset();
            }

            // 5. wait_for_last_vote -> committed (others committed received)
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 3;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = nextBallotNo - 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);

                // last vote message
                string decreeContent1 = "test2";
                for (int i = 0; i < 5; i++)
                {
                    LastVoteMessage lastVote = null;
                    if (i == 1)
                    {
                        lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo, nextBallotNo,
                            3/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                        lastVote.Commited = true;
                    }
                    else
                    {
                        lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo, nextBallotNo,
                            0/*votedBallotNo*/, null/*votedDecree*/);
                    }

                    await proposer.HandlePaxosMessage(lastVote);
                    if (i >= 1)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        var result = await collectLastVoteTask;
                        Assert.AreEqual(propose.State, ProposeState.ReadyToCommit);
                        Assert.AreEqual(propose.LastTriedBallot, nextBallotNo);
                        Assert.AreEqual(propose.Decree.Content, decreeContent1);

                        //VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.Commited, decreeContent1);

                        //Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 1);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, i + 1);

                        var returnedLastVote = propose.LastVoteMessages[i];

                        VerifyLastVoteMessage(returnedLastVote, nextDecreeNo, nextBallotNo, 0/*votedBallotNo*/, null/*votedContent*/);

                        VerifyPropose(propose, nextBallotNo/*lastTriedBallot*/, ProposeState.WaitForLastVote, decreeContent);

                        Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                    }
                }
                var collectLastVoteResult = await collectLastVoteTask;
                Assert.AreEqual(propose.State, ProposeState.ReadyToCommit);
                Assert.AreEqual(propose.Decree.Content, decreeContent1);

                proposeManager.Reset();
            }

            // 6. wait_for_last_vote -> collect_last_vote (stale message)
            {
                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 3;
                var propose = proposeManager.AddPropose(nextDecreeNo, (ulong)cluster.Members.Count);
                propose.LastTriedBallot = nextBallotNo - 1;
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                string decreeContent = "test1";
                var collectLastVoteTask = proposer.CollectLastVote(
                    new ConsensusDecree() { Content = decreeContent },
                    nextDecreeNo);

                for (int i = 0; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForLastVote);

                // last vote message
                string decreeContent1 = "test2";
                for (int i = 0; i < 5; i++)
                {
                    if (i == 1)
                    {
                        var stateMsg = CreateStaleMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo, nextBallotNo,
                            5/*nextBallotNo*/);
                        await proposer.HandlePaxosMessage(stateMsg);
                    }
                    else
                    {
                        var lastVote = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo, nextBallotNo,
                            2/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                        await proposer.HandlePaxosMessage(lastVote);
                    }

                    if (i >= 1)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        break;
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                    }
                    Assert.AreEqual(propose.LastVoteMessages.Count, i + 1);

                    var returnedLastVote = propose.LastVoteMessages[i];

                    VerifyLastVoteMessage(returnedLastVote, nextDecreeNo, nextBallotNo, 2/*votedBallotNo*/, decreeContent1/*votedContent*/);

                    VerifyPropose(propose, nextBallotNo/*lastTriedBallot*/, ProposeState.WaitForLastVote, decreeContent1);
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);
                var collectLastVoteResult = await collectLastVoteTask;
                Assert.AreEqual(propose.GetNextBallot(), (ulong)5);
            }

            foreach (var rpcserver in serverList)
            {
                await rpcserver.Stop();
            }

            networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            msgList = new List<List<RpcMessage>>();
            serverList = new List<RpcServer>();
            foreach (var nodeAddr in cluster.Members)
            {
                var svrAddr = nodeAddr;
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }
            rpcClient = new RpcClient();


            // 7. beginnewballot -> wait_for_new_ballot_result
            {
                await proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent = "test1";

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 4;

                var propose = new Propose((ulong)cluster.Members.Count, nextBallotNo,
                    new ConsensusDecree(decreeContent), ProposeState.BeginNewBallot);
                proposeManager.AddPropose(nextDecreeNo, propose);

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                var beginBallotTask = proposer.BeginNewBallot(nextDecreeNo, nextBallotNo);
                for (int i = 1; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var beginBallot = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallot);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForNewBallotVote);

                await proposerNote.Clear();
                proposeManager.Reset();
            }

            // 8. wait_for_new_ballot_result -> readyCommmit (enough vote msg received)
            {
                await proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent = "test1";

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 4;

                var propose = new Propose((ulong)cluster.Members.Count, nextBallotNo,
                    new ConsensusDecree(decreeContent), ProposeState.BeginNewBallot);
                proposeManager.AddPropose(nextDecreeNo, propose);

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                Assert.AreEqual(propose.State, ProposeState.BeginNewBallot);
                var beginBallotTask = proposer.BeginNewBallot(nextDecreeNo, nextBallotNo);
                for (int i = 1; i < cluster.Members.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var beginBallot = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallot);
                    msgList[i].Clear();
                }
                Assert.AreEqual(propose.State, ProposeState.WaitForNewBallotVote);

                for (int i = 1; i < 5; i++)
                {
                    var voteMsg = CreateVoteMessage(
                        NodeAddress.Serialize(cluster.Members[i]),
                        NodeAddress.Serialize(cluster.Members[0]),
                        nextDecreeNo, nextBallotNo);
                    await proposer.HandlePaxosMessage(voteMsg);
                    if (i < 3)
                    {
                        Assert.AreEqual(propose.VotedMessages.Count, i);
                        Assert.IsFalse(beginBallotTask.IsCompleted);
                    }
                    else
                    {
                        Assert.AreEqual(propose.VotedMessages.Count, 3);
                        await Task.Delay(500);
                        Assert.IsTrue(beginBallotTask.IsCompleted);
                    }
                }
                var beginBallotResult = await beginBallotTask;
                Assert.AreEqual(propose.State, ProposeState.ReadyToCommit);
                Assert.AreEqual(propose.Decree.Content, decreeContent);


                await proposerNote.Clear();
                proposeManager.Reset();
            }

            // 9. wait_for_new_ballot_result -> BeginNewBallot (timeout)
            {
                await proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent = "test1";

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 4;

                var propose = new Propose((ulong)cluster.Members.Count, nextBallotNo,
                    new ConsensusDecree(decreeContent), ProposeState.WaitForNewBallotVote);
                proposeManager.AddPropose(nextDecreeNo, propose);

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);

                await Task.Delay(3000); // resp timout is 2s


                Assert.AreEqual(propose.State, ProposeState.BeginNewBallot);


                await proposerNote.Clear();
                proposeManager.Reset();
            }

            // 10. wait_for_new_ballot_result -> committed (others committed received).
            {
                await proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent1 = "test0";
                string decreeContent = "test1";

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 4;

                var propose = new Propose((ulong)cluster.Members.Count, nextBallotNo,
                    new ConsensusDecree(decreeContent), ProposeState.WaitForNewBallotVote);
                proposeManager.AddPropose(nextDecreeNo, propose);

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);


                for (int i = 1; i < 5; i++)
                {
                    if (i == 2)
                    {
                        var lastVoteMessage = CreateLastVoteMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo, nextBallotNo,
                            4/*votedBallotNo*/,
                            decreeContent1);
                        lastVoteMessage.Commited = true;
                        await proposer.HandlePaxosMessage(lastVoteMessage);

                        // every thing will be reset, and new query last vote message will be send
                        break;
                    }
                    var voteMsg = CreateVoteMessage(
                        NodeAddress.Serialize(cluster.Members[i]),
                        NodeAddress.Serialize(cluster.Members[0]),
                        nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/);
                    await proposer.HandlePaxosMessage(voteMsg);
                    Assert.AreEqual(propose.VotedMessages.Count, i);
                }
                Assert.AreEqual(propose.State, ProposeState.ReadyToCommit);
                Assert.AreEqual(propose.Decree.Content, decreeContent1);


                await proposerNote.Clear();
                proposeManager.Reset();
            }

            // 11. wait_for_new_ballot_result -> collect_last_vote (stale message received)
            {
                await proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent = "test1";

                ulong nextDecreeNo = 1;
                ulong nextBallotNo = 4;

                var propose = new Propose((ulong)cluster.Members.Count, nextBallotNo,
                    new ConsensusDecree(decreeContent), ProposeState.WaitForNewBallotVote);
                proposeManager.AddPropose(nextDecreeNo, propose);

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                Assert.AreEqual(propose.State, ProposeState.WaitForNewBallotVote);

                for (int i = 1; i < 5; i++)
                {
                    if (i == 2)
                    {
                        var staleMsg = CreateStaleMessage(
                            NodeAddress.Serialize(cluster.Members[i]),
                            NodeAddress.Serialize(cluster.Members[0]),
                            nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/,
                            5/*nextBallotNo*/);
                        await proposer.HandlePaxosMessage(staleMsg);
                        // every thing will be reset, and new query last vote message will be send
                        break;
                    }
                    var voteMsg = CreateVoteMessage(
                        NodeAddress.Serialize(cluster.Members[i]),
                        NodeAddress.Serialize(cluster.Members[0]),
                        nextDecreeNo/*decreeNo*/, nextBallotNo/*ballotNo*/);
                    await proposer.HandlePaxosMessage(voteMsg);
                    Assert.AreEqual(propose.VotedMessages.Count, i);
                }

                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);
                VerifyPropose(propose, 5, ProposeState.QueryLastVote, null); // ballot already updated to ballot returned by stale message
                Assert.AreEqual(propose.LastVoteMessages.Count, 0);
                Assert.AreEqual(propose.VotedMessages.Count, 1);

                await proposerNote.Clear();
                proposeManager.Reset();
            }

            // 12. readyCommit -> committed (commit)
        }

        [TestMethod()]
        public async Task StateMachineTest()
        {
            CleanupLogFiles(null);

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 128 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            //NetworkFactory.SetNetworkCreator(new TcpNetworkCreator());

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                tableNodeMap[ConsensusNodeHelper.GetInstanceName(nodeAddr)] = node;
            }


            var master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            await master.InstertTable(new ReplicatedTableRequest() { Key = "1", Value = "test1" });
            await master.InstertTable(new ReplicatedTableRequest() { Key = "2", Value = "test2" });
            await master.InstertTable(new ReplicatedTableRequest() { Key = "3", Value = "test3" });

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }

        [TestMethod()]
        public async Task StateMachineCheckpointTest()
        {
            CleanupLogFiles(null);

            Dictionary<string, string> _table = new Dictionary<string, string>();
            var start = DateTime.Now;
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 138 + i);
                cluster.Members.Add(nodeAddr);

                var instanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);

                var metaLogFile = ".\\storage\\" + instanceName + ".meta";
                var proposerLogFile = ".\\storage\\" + instanceName + ".proposerlog";
                var votedLogFile = ".\\storage\\" + instanceName + ".voterlog";
                File.Delete(proposerLogFile);
                File.Delete(votedLogFile);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                var instanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);
                tableNodeMap[instanceName] = node;
            }

            start = DateTime.Now;

            var master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            for (int i = 0; i < 500; i++)
            {
                var task = master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                await task;
            }

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }


            end = DateTime.Now;


            networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            foreach (var nodeAddr in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                var nodeInstanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);
                tableNodeMap[nodeInstanceName] = node;
                var proposerLogFile = ".\\storage\\" + nodeInstanceName + ".proposerlog";
                var votedLogFile = ".\\storage\\" + nodeInstanceName + ".voterlog";

                var metaLogFile = ".\\storage\\" + nodeInstanceName + ".meta";
                await node.Load(metaLogFile);
                //await node.Load(proposerLogFile, votedLogFile);
            }

            start = DateTime.Now;

            master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];

            costTime = (end - start).TotalMilliseconds;
            Console.WriteLine("TPS: {0}", 10000 * 1000 / costTime);

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }

        [TestMethod()]
        public async Task StateMachineCheckpointRequestTest()
        {
            CleanupLogFiles(null);

            Dictionary<string, string> _table = new Dictionary<string, string>();
            var start = DateTime.Now;
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 148 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            int unHealthyNodeIndex = 2;
            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            for (int i = 0; i < cluster.Members.Count; ++i)
            {
                if (i == unHealthyNodeIndex)
                {
                    //continue;
                }
                var nodeAddr = cluster.Members[i];
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                var nodeInstanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);
                tableNodeMap[nodeInstanceName] = node;
            }

            start = DateTime.Now;

            var master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            for (int i = 0; i < 500; i++)
            {
                var task = master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                await task;
                if (i == 124)
                {
                    Console.WriteLine("break");
                }
            }
            await Task.Delay(5000); // checkpoint now is a sync task
            foreach(var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }

            var unHealthyNodeAddr = cluster.Members[unHealthyNodeIndex];
            var unhealthInstanceName = ConsensusNodeHelper.GetInstanceName(unHealthyNodeAddr);
            var unHealthyNodeMetaLogFile = ".\\storage\\" + unhealthInstanceName + ".meta";
            var unHealthyNodeProposerLogFile = ".\\storage\\" + unhealthInstanceName + ".proposerlog";
            var unHealthyVotedLogFile = ".\\storage\\" + unhealthInstanceName + ".voterlog";


            CleanupLogFiles(unhealthInstanceName);

            networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            ReplicatedTable.ReplicatedTable unHealthyNode = null;
            for (int i = 0; i < cluster.Members.Count; ++i)
            {
                if (i == unHealthyNodeIndex)
                {
                    continue;
                }
                tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[i])] = new ReplicatedTable.ReplicatedTable(cluster, cluster.Members[i]);
            }
            tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[unHealthyNodeIndex])] = new ReplicatedTable.ReplicatedTable(cluster, cluster.Members[unHealthyNodeIndex]);

            for (int i = 0; i < cluster.Members.Count; ++i)
            {
                var nodeAddr = cluster.Members[i];
                var nodeInstanceName = ConsensusNodeHelper.GetInstanceName(nodeAddr);
                var node = tableNodeMap[nodeInstanceName];
                var proposerLogFile = ".\\storage\\" + nodeInstanceName + ".proposerlog";
                var votedLogFile = ".\\storage\\" + nodeInstanceName + ".voterlog";

                var metaLogFile = ".\\storage\\" + nodeInstanceName + ".meta";
                if (i == unHealthyNodeIndex)
                {
                    unHealthyNode = node;
                }
                else
                {
                    await node.Load(metaLogFile);
                }
            }

            unHealthyNode.MissedRequestTimeoutInSecond = 5;
            await unHealthyNode.Load(unHealthyNodeMetaLogFile);
            for (int i = 0; i < 500; i++)
            {
                var key = i.ToString();
                var value = await unHealthyNode.ReadTable(key);
                Assert.IsNull(value);
            }
            int rowIndex = 501;
            var task1 = unHealthyNode.InstertTable(new ReplicatedTableRequest() { Key = rowIndex.ToString(), Value = "test" + rowIndex.ToString() });
            await task1;

            await Task.Delay(10 * 1000);

            for (int i = 0; i < 500; i++)
            {
                var key = i.ToString();
                var value = await unHealthyNode.ReadTable(key);
                Assert.AreEqual(value, "test" + i.ToString());
            }

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }

            CleanupLogFiles(unhealthInstanceName);
        }

        [TestMethod()]
        public async Task RSLServerTest()
        {
            CleanupLogFiles(null);

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 158 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            //NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            NetworkFactory.SetNetworkCreator(new TcpNetworkCreator());

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                tableNodeMap[ConsensusNodeHelper.GetInstanceName(nodeAddr)] = node;
            }

            var master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];
            var serviceServer = new PineHillRSL.ServerLib.PineHillRSLServer(master);
            var serviceAddr = new NodeAddress(new NodeInfo("127.0.0.1"), 1000);
            await serviceServer.StartServer(serviceAddr);

            var client = new ClientLib.PineHillRSLClient(null);
            await client.InsertTable("1", "test1");
            await client.InsertTable("2", "test2");
            await client.InsertTable("3", "test3");

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }

        private PaxosMessage CreatePaxosMessage(RpcMessage rpcMessage)
        {
            var paxosRpcMessage = PaxosRpcMessageFactory.CreatePaxosRpcMessage(rpcMessage);
            var paxosMessage = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMessage);
            return paxosMessage;
        }
        
        private LastVoteMessage CreateLastVoteMessage(string srcNode, string targetNode, ulong decreeNo, ulong ballotNo, ulong votedBallotNo, string votedDecree)
        {
            var lastVote = new LastVoteMessage();
            lastVote.SourceNode = srcNode;
            lastVote.TargetNode = targetNode;
            lastVote.DecreeNo = decreeNo;
            lastVote.BallotNo = ballotNo;
            lastVote.VoteBallotNo = votedBallotNo; // never vote
            lastVote.VoteDecreeContent = votedDecree;

            return lastVote;

        }

        private VoteMessage CreateVoteMessage(string srcNode, string targetNode, ulong decreeNo, ulong ballotNo)
        {
            return new VoteMessage()
            {
                DecreeNo = decreeNo,
                BallotNo = ballotNo,
                SourceNode = srcNode,
                TargetNode = targetNode
            };
        }

        private StaleBallotMessage CreateStaleMessage(string srcNode, string targetNode, ulong decreeNo, ulong ballotNo, ulong nextBallotNo)
        {
            return new StaleBallotMessage()
            {
                DecreeNo = decreeNo,
                BallotNo = ballotNo,
                SourceNode = srcNode,
                TargetNode = targetNode,
                NextBallotNo = nextBallotNo
            };
        }

        private void VerifyLastVoteMessage(LastVoteMessage lastVoteMsg, ulong decreeNo, ulong ballotNo, ulong voteBallotNo, string votedDecreeContent)
        {
            Assert.AreEqual(lastVoteMsg.DecreeNo, decreeNo);
            Assert.AreEqual(lastVoteMsg.BallotNo, ballotNo);
            Assert.AreEqual(lastVoteMsg.VoteBallotNo, voteBallotNo);
            if (votedDecreeContent == null)
            {
                Assert.IsNull(lastVoteMsg.VoteDecreeContent);
            }
            else
            {
                Assert.IsNotNull(lastVoteMsg.VoteDecreeContent);
                Assert.AreEqual(lastVoteMsg.VoteDecreeContent, votedDecreeContent);
            }
        }

        private void VerifyPropose(Propose propose, ulong lastTriedBallot, ProposeState state, string decreeContent)
        {
            Assert.AreEqual(propose.LastTriedBallot, lastTriedBallot);
            // ongoing decree is decreeContent
            if (decreeContent == null)
            {
                Assert.IsTrue(propose.Decree == null || String.IsNullOrEmpty(propose.Decree.Content));
            }
            else
            {
                Assert.AreEqual(propose.Decree.Content, decreeContent);
            }
            Assert.AreEqual(propose.State, state);

        }

        [TestMethod()]
        public async Task StateMachinePefTest()
        {
            //ThreadPool.SetMinThreads(100, 100);
            System.Net.ServicePointManager.DefaultConnectionLimit = 200;
            Trace.WriteLine("Begin StateMachinePerfTest");
            Console.WriteLine("Begin StateMachinePerfTest");
            CleanupLogFiles(null);
            Dictionary<string, string> _table = new Dictionary<string, string>();
            var start = DateTime.Now;
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            LogSizeThreshold.CommitLogFileCheckpointThreshold = 100 * 1024;

            Trace.WriteLine("Cleaned log files");
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 168 + i);
                cluster.Members.Add(nodeAddr);
            }

            //var networkInfr = new TestNetworkInfr();
            //NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            NetworkFactory.SetNetworkCreator(new TcpNetworkCreator());

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeAddr in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                tableNodeMap[ConsensusNodeHelper.GetInstanceName(nodeAddr)] = node;
            }
            Trace.WriteLine("All cluster nodes intialized");

            start = DateTime.Now;

            List<Task> taskList = new List<Task>();
            var master = tableNodeMap[ConsensusNodeHelper.GetInstanceName(cluster.Members[0])];

            var finishedRequestCount = 0;
            for (int i = 0; i < 100000; i++)
            {
                var task =  master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                taskList.Add(task);
                if (taskList.Count > 150)
                {
                    //await Task.WhenAll(taskList);
                    while(taskList.Count > 0)
                    {
                        var finishedTask = await Task.WhenAny(taskList);
                        taskList.Remove(finishedTask);
                        finishedRequestCount++;
                    }
                    Trace.WriteLine($"Finished {finishedRequestCount} rows");
                    taskList.Clear();
                }
            }
            while (taskList.Count > 0)
            {
                var finishedTask = await Task.WhenAny(taskList);
                taskList.Remove(finishedTask);
                finishedRequestCount++;
            }
            Trace.WriteLine($"Finished {finishedRequestCount} rows");
            //await Task.WhenAll(taskList);

            end = DateTime.Now;

            costTime = (end - start).TotalMilliseconds;
            Trace.WriteLine($"TPS: {10000 * 1000 / costTime}");

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }
            LogSizeThreshold.ResetDefault();
        }

        /*
        [TestMethod()]
        public async Task PaxosPefTest()
        {
            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeInfo);
                nodeMap[nodeInfo.Name] = node;
            }

            bool isParallel = true;
            var start = DateTime.Now;

            var proposer = nodeMap[cluster.Members[0].Name];

            int workerCount = 0;
            int iocpCount = 0;
            ThreadPool.GetMinThreads(out workerCount, out iocpCount);
            var currentThreadCount = ThreadPool.ThreadCount;

            Statistic collectLastVoteTime = new Statistic();
            Statistic voteTime = new Statistic();
            Statistic commitTime = new Statistic();
            Statistic getProposeTime = new Statistic();
            Statistic getProposeLockTime = new Statistic();
            Statistic prepareNewBallotTime = new Statistic();
            Statistic broadcaseQueryLastVoteTime = new Statistic();

            Statistic proposeDecreeTime = new Statistic();
            Statistic taskCreateTime = new Statistic();

            string randomstr = new string('t', 1024 * 50);
            var randomData = Encoding.UTF8.GetBytes(randomstr);
            var prefixData = Encoding.UTF8.GetBytes("test");

            DateTime beginWait = DateTime.Now;
            var taskList = new List<Task>();
            var reqList = new List<int>();
            for (int i = 0; i < 5000; i++)
            {
                if (false)
                {
                    reqList.Add(i);
                    if (reqList.Count < 50)
                    {
                        continue;
                    }
                    beginWait = DateTime.Now;
                    Parallel.ForEach(reqList, (int val) =>
                    {
                        var decree = new ConsensusDecree()
                        {
                            Content = "test" + val.ToString()
                        };
                        //var task = Task.Run(async () =>
                        //{
                        var begin = DateTime.Now;
                        var task = proposer.ProposeDecree(decree, decreeNo:0);
                        var result = task.Result;
                        var proposeTime = DateTime.Now - begin;
                        proposeDecreeTime.Accumulate(proposeTime.TotalMilliseconds);
                        collectLastVoteTime.Accumulate(result.CollectLastVoteTimeInMs.TotalMilliseconds);
                        voteTime.Accumulate(result.VoteTimeInMs.TotalMilliseconds);
                        commitTime.Accumulate(result.CommitTimeInMs.TotalMilliseconds);
                        getProposeTime.Accumulate(result.GetProposeCostTime.TotalMilliseconds);
                        getProposeLockTime.Accumulate(result.GetProposeLockCostTime.TotalMilliseconds);
                        prepareNewBallotTime.Accumulate(result.PrepareNewBallotCostTime.TotalMilliseconds);
                        broadcaseQueryLastVoteTime.Accumulate(result.BroadcastQueryLastVoteCostTime.TotalMilliseconds);
                        //});

                    });
                    var waitTime = DateTime.Now - beginWait;
                    reqList.Clear();
                }
                else
                {
                    var data = new byte[prefixData.Length + sizeof(int) + randomData.Length];
                    Buffer.BlockCopy(prefixData, 0, data, 0, prefixData.Length);
                    Buffer.BlockCopy(BitConverter.GetBytes(i), 0, data, prefixData.Length, sizeof(int));
                    var decree = new ConsensusDecree()
                    {
                        Data = data
                        //Content = "test" + i.ToString() + randomstr
                    };
                    var beforeCreateTaskTime = DateTime.Now;
                    var task = Task.Run(async () =>
                    {
                        var begin = DateTime.Now;
                        var result = await proposer.ProposeDecree(decree, decreeNo: 0);
                        var proposeTime = DateTime.Now - begin;
                        proposeDecreeTime.Accumulate(proposeTime.TotalMilliseconds);
                        collectLastVoteTime.Accumulate(result.CollectLastVoteTimeInMs.TotalMilliseconds);
                        voteTime.Accumulate(result.VoteTimeInMs.TotalMilliseconds);
                        commitTime.Accumulate(result.CommitTimeInMs.TotalMilliseconds);
                        getProposeTime.Accumulate(result.GetProposeCostTime.TotalMilliseconds);
                        getProposeLockTime.Accumulate(result.GetProposeLockCostTime.TotalMilliseconds);
                        prepareNewBallotTime.Accumulate(result.PrepareNewBallotCostTime.TotalMilliseconds);
                        broadcaseQueryLastVoteTime.Accumulate(result.BroadcastQueryLastVoteCostTime.TotalMilliseconds);


                    });
                    var afterCreateTaskTime = DateTime.Now;
                    taskCreateTime.Accumulate((afterCreateTaskTime - beforeCreateTaskTime).TotalMilliseconds);
                    if (isParallel)
                    {
                        taskList.Add(task);
                        if (taskList.Count > 500)
                        {
                            //await Task.WhenAll(taskList);
                            DateTime firstFinishTime = DateTime.MaxValue;
                            while (taskList.Count > 0)
                            {
                                var finishedIndex = Task.WaitAny(taskList.ToArray());
                                taskList.RemoveAt(finishedIndex);
                                if (DateTime.Now < firstFinishTime)
                                {
                                    firstFinishTime = DateTime.Now;
                                }
                            }
                            var firstWaitTime = firstFinishTime - beginWait;
                            var waitTime = DateTime.Now - beginWait;
                            taskList.Clear();
                            beginWait = DateTime.Now;

                            if (i > 500)
                            {
                                Trace.WriteLine("Finished" + i.ToString());
                                Debug.WriteLine("Finished" + i.ToString());
                                Console.WriteLine("Finished" + i.ToString());

                            }
                        }
                    }
                    else
                    {
                        await task;
                    }

                }
            }

            if (true)
            {
                if (isParallel)
                {
                    await Task.WhenAll(taskList);
                }

            }
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            foreach (var node in nodeMap)
            {
                await node.Value.DisposeAsync();
            }
        }
        [TestMethod()]
        public async Task MultiTaskPefTest()
        {
            int concurrentCount = 40;
            bool matchCal = true;

            var data = Encoding.ASCII.GetBytes("aldjfalkdfjlkasdjflkasdjfklsadjflkasjdfklasdjflasdjf");
            var str = Encoding.ASCII.GetString(data);

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < concurrentCount; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 1000; i++)
                    {
                        var task = Task.Run(async () =>
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                await Task.Run(() =>
                                {
                                    if (matchCal)
                                    {
                                        double pi = 3.1414926;
                                        double d = 1414341324;
                                        double area = pi * (d / 2) * (d / 2);
                                        double circle = pi * d;
                                        //var nextBallotMessage = new NextBallotMessage();
                                        //var tmpdata = new byte[128];
                                        //memList.Add(tmpdata);
                                    }
                                    else
                                    {
                                        // var str = Encoding.ASCII.GetString(data);
                                        //var result = Encoding.ASCII.GetBytes(str);
                                        var dest = System.Runtime.InteropServices.Marshal.AllocHGlobal(data.Length + 1);
                                        System.Runtime.InteropServices.Marshal.Copy(data, 0, dest, data.Length);

                                        //var str = System.Convert.ToBase64String(data);
                                        //var result = System.Convert.FromBase64String(str);
                                        //var nextBallotMessage = new NextBallotMessage();
                                        //nextBallotMessage.TargetNode = "testnode";
                                        //nextBallotMessage.DecreeNo = 2;
                                        //nextBallotMessage.BallotNo = 2;

                                        //nextBallotMessage.SourceNode = "testnode";
                                        //var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(nextBallotMessage);
                                        //var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                                    }

                                });
                            }

                        });
                        tasks.Add(task);
                        await Task.WhenAll(tasks);
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        [TestMethod()]
        public async Task PaxosNetworkPefTest()
        {
            var cluster = new ConsensusCluster();
            List<NodeAddress> nodeAddrList = new List<NodeAddress>();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
                nodeAddrList.Add(new NodeAddress(node, 0));
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));
            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;
            var srcServerAddress = new NodeAddress(new NodeInfo(sourceNode), 88);
            var srcClientAddress = new NodeAddress(new NodeInfo(sourceNode), 0);
            var targetServerAddress = new NodeAddress(new NodeInfo(targetNode), 88);
            var targetClientAddress = new NodeAddress(new NodeInfo(targetNode), 0);

            List<List<RpcMessage>> msgList = new List<List<RpcMessage>>();
            List<RpcServer> serverList = new List<RpcServer>();
            foreach (var node in nodeAddrList)
            {
                var svrAddr = new NodeAddress(node.Node, 88);
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }

            List<RpcMessage> rpcMessages = new List<RpcMessage>();
            foreach (var node in cluster.Members)
            {
                var nextBallotMessage = new NextBallotMessage();
                nextBallotMessage.TargetNode = node.Name;
                nextBallotMessage.DecreeNo = 2;
                nextBallotMessage.BallotNo = 2;

                nextBallotMessage.SourceNode = cluster.Members[0].Name;
                var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(nextBallotMessage);
                var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                rpcMessages.Add(rpcMsg);
            }

            List<Task<RpcMessage>> tasks = new List<Task<RpcMessage>>();
            var rpcClient = new RpcClient(srcClientAddress);
            {
                bool isParallel = true;
                var start = DateTime.Now;
                for (int round = 0; round < 100000; round++)
                {
                    // 1. collect decree for this instance, send NextBallotMessage
                    //foreach (var node in cluster.Members)
                    for (int i = 1; i < cluster.Members.Count; i++)
                    {
                        var node = cluster.Members[i];

                        var nextBallotMessage = new NextBallotMessage();
                        nextBallotMessage.TargetNode = node.Name;
                        nextBallotMessage.DecreeNo = 2;
                        nextBallotMessage.BallotNo = 2;


                        nextBallotMessage.SourceNode = cluster.Members[0].Name;
                        var paxosRpcMsg = PaxosMessageFactory.CreatePaxosRpcMessage(nextBallotMessage);
                        var rpcMsg = PaxosRpcMessageFactory.CreateRpcRequest(paxosRpcMsg);
                        //rpcMsg.NeedResp = false;
                        var remoteAddr = new NodeAddress(new NodeInfo(node.Name), 88);
                        if (isParallel)
                        {
                            tasks.Add(rpcClient.SendRequest(remoteAddr, rpcMsg));
                            if (tasks.Count > 500000)
                            {
                                await Task.WhenAll(tasks);
                                tasks.Clear();
                            }
                        }
                        else
                        {
                            await rpcClient.SendRequest(remoteAddr, rpcMsg);
                        }
                    }
                }
                if (isParallel)
                {
                    await Task.WhenAll(tasks);
                }
                var end = DateTime.Now;
                var costTime = (end - start).TotalMilliseconds;
            }

            var pefCounter = PerfCounterManager.GetInst();
            pefCounter.GetCounterValue(0);
        }*/

        private void CleanupLogFiles(string nodeName)
        {
            List<string> paths = new List<string>(Directory.EnumerateFiles(".\\storage"));
            foreach (var path in paths)
            {
                if (string.IsNullOrEmpty(nodeName) || path.IndexOf(nodeName) != -1)
                {
                    File.Delete(path);
                }
            }

        }
    }
}

