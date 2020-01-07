using Microsoft.VisualStudio.TestTools.UnitTesting;
using Paxos.Message;
using Paxos.Network;
using Paxos.Notebook;
using Paxos.Protocol;
using Paxos.Persistence;
using Paxos.Node;
using Paxos.Request;
using Paxos.Rpc;
using Paxos.ReplicatedTable;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
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
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeInfo);
                nodeMap[nodeInfo.Name] = node;
            }


            var proposer = nodeMap[cluster.Members[0].Name];
            var decree = new PaxosDecree()
            {
                Content = "test"
            };
            var result = await proposer.ProposeDecree(decree, 0/*nextDecreNo*/);
            var readReslut = await proposer.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 1);

            var proposer2 = nodeMap[cluster.Members[1].Name];

            var decree2 = new PaxosDecree()
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
            var decree3 = new PaxosDecree()
            {
                Content = "test3"
            };
            result = await proposer2.ProposeDecree(decree3, 0);
            Assert.IsTrue(result.Decree.Content.Equals("test3"));
            Assert.IsTrue(result.DecreeNo == 3);
            await networkInfr.WaitUntillAllReceivedMessageConsumed();
            readReslut = await proposer.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            readReslut = await proposer2.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            foreach(var node in nodeMap)
            {
                node.Value.Dispose();
            }
        }

        [TestMethod()]
        public async Task PaxosNodeBootstrapTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);

                var proposerLogFile = ".\\loegger" + node.Name + ".log";
                var votedLogFile = ".\\votedlogger_" + node.Name + ".log";
                File.Delete(proposerLogFile);
                File.Delete(votedLogFile);
            }

            await BeginNewProposeTest();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeInfo);
                nodeMap[nodeInfo.Name] = node;
                var proposerLogFile = ".\\loegger" + nodeInfo.Name + ".log";
                var votedLogFile = ".\\votedlogger_" + nodeInfo.Name + ".log";
                await node.Load(proposerLogFile, votedLogFile);
            }

            var proposer = nodeMap[cluster.Members[0].Name];

            var readReslut = await proposer.ReadDecree(1);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            var proposer2 = nodeMap[cluster.Members[1].Name];

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
                node.Value.Dispose();
            }
        }

        [TestMethod()]
        public async Task VoteRoleTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;
            var srcServerAddress = new NodeAddress(new NodeInfo(sourceNode), 88);
            var srcClientAddress = new NodeAddress(new NodeInfo(sourceNode), 0);
            var targetServerAddress = new NodeAddress(new NodeInfo(targetNode), 88);
            var targetClientAddress = new NodeAddress(new NodeInfo(targetNode), 0);

            var logPrefix = Guid.NewGuid().ToString();

            var ledgerLogger = new FilePaxosCommitedDecreeLog(logPrefix + "logger_node1.log");
            var votedLogger = new FilePaxosVotedBallotLog(logPrefix + "votedlogger_node1.log");
            var proposerNote = new ProposerNote(ledgerLogger);
            var voterNote = new VoterNote(votedLogger);

            var msgList = new List<RpcMessage>();
            // 1. NextBallotMessage
            {
                // 1.1. voter have voted no ballot for a decree
                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                var rpcClient = new RpcClient(targetClientAddress);
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, voterNote, proposerNote);
                var nextBallotMsg = new NextBallotMessage();
                nextBallotMsg.DecreeNo = 1;
                nextBallotMsg.BallotNo = 1;
                nextBallotMsg.SourceNode = sourceNode;
                nextBallotMsg.TargetNode = targetNode;
                await voter.DeliverNextBallotMessage(nextBallotMsg);

                var propserConnection = networkInfr.GetConnection(srcServerAddress, targetClientAddress);
                Assert.AreEqual(msgList.Count, 1);
                var lastVoteMsg = CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.IsNull(lastVoteMsg.VoteDecree);
                Assert.AreEqual(voterNote.GetNextBallotNo(nextBallotMsg.DecreeNo), (ulong)1);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                // now the NextBalloNo is 1
                // 1.2. New ballot no > 1 will be accepted and got a last vote
                nextBallotMsg.BallotNo = 2;
                await voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.IsNull(lastVoteMsg.VoteDecree);
                Assert.AreEqual(voterNote.GetNextBallotNo(nextBallotMsg.DecreeNo), (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                // now NextBallotNo is 2

                // 1.3. NextBallotNo <= 2 will not be accepted and got a stale ballot message
                await voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg =  CreatePaxosMessage(msgList[0]) as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)2);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                nextBallotMsg.BallotNo = 1;
                await voter.DeliverNextBallotMessage(nextBallotMsg);
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
                    new PaxosDecree(){ Content = voteContent });

                nextBallotMsg.BallotNo = 3;
                await voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)3);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecree.Content, voteContent);
                Assert.IsFalse(lastVoteMsg.Commited);
                msgList.Clear();

                //1.5 decree has been committed in ledger
                await proposerNote.CommitDecree(1, lastVoteMsg.VoteDecree);
                nextBallotMsg.BallotNo = 2; // do not care about the ballot no for committed decree

                await voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg =  CreatePaxosMessage(msgList[0]) as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                //Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecree.Content, voteContent);
                Assert.IsTrue(lastVoteMsg.Commited);
                msgList.Clear();

                // cleanup
                proposerNote.Clear();

            }

            // 2. StartNewBallotMessage
            {
                voterNote.Reset();

                networkInfr = new TestNetworkInfr();
                NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                // 2.2 has no NextBallotNo yet
                var rpcClient = new RpcClient(targetClientAddress);
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, voterNote, proposerNote);

                var propserConnection = networkInfr.GetConnection(srcServerAddress, targetClientAddress);

                string voteContent = "test1";
                var beginBallotMsg = new BeginBallotMessage();
                beginBallotMsg.DecreeNo = 1;
                beginBallotMsg.BallotNo = 1;
                beginBallotMsg.SourceNode = sourceNode;
                beginBallotMsg.TargetNode = targetNode;
                beginBallotMsg.Decree = new PaxosDecree()
                {
                    Content = voteContent
                };
                await voter.DeliverBeginBallotMessage(beginBallotMsg);    // no response
                Assert.AreEqual(msgList.Count, 0);

                await voterNote.UpdateNextBallotNo(1, 2); // nextBallotNo = 2, > 1
                await voter.DeliverBeginBallotMessage(beginBallotMsg);    // state ballot response
                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg =  CreatePaxosMessage(msgList[0]) as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);    //nextBallotNo > ballotNo 
                msgList.Clear();


                // 2.2 NextBallotNo match
                beginBallotMsg.BallotNo = 2;
                await voter.DeliverBeginBallotMessage(beginBallotMsg);    // vote
                Assert.AreEqual(msgList.Count, 1);
                var voteMsg =  CreatePaxosMessage(msgList[0]) as VoteMessage;
                Assert.IsNotNull(voteMsg);
                Assert.AreEqual(voteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(voteMsg.BallotNo, (ulong)2);

                msgList.Clear();

                // 2.3 Decree committed, return a lastvotemsg, indicate the decree committed
                await proposerNote.CommitDecree(beginBallotMsg.DecreeNo, beginBallotMsg.Decree);
                beginBallotMsg.BallotNo = 3;
                await voter.DeliverBeginBallotMessage(beginBallotMsg);    // vote
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
        }

        [TestMethod()]
        public async Task ProposerRoleTest()
        {
            /// <summary>
            /// Three phase commit (3PC)
            /// 1. query existing vote
            /// 2. prepare commit
            /// 3. commit
            /// 
            ///     CurrentState            Event                   NextState               Action
            ///     Init                    Propose                 QueryLastVote           Query lastvote from cluster
            ///     QueryLastVote           ReceiveLastVote         BeginNewBallot(enough)  Send BeginNewBallot to quorum
            ///                                                     QueryLastVote(x)        Nothing
            ///                                                     BeginCommit(Committed)   Send commit to missing decree node
            ///     QueryLastVote           Timeout                 QueryLastVote           Query lastvote with new ballotNo
            ///     BeginNewBallot          ReceiveVote             BeginCommit(enough)     Send commit to all nodes
            ///                                                     ReceiveVote(x)          Nothing
            ///     BeginNewBallot          Timeout                 QueryLastVote           Send BeginNewBallot with a new ballotNo
            ///     BeginCommit             Response                Committed(enough)        Call subscriber
            ///                                                     BeginCommit(x)
            ///     BeginCommit             Timeout                 BeginCommit             Send commit to all nodes
            ///     
            /// </summary>

            var cluster = new PaxosCluster();
            List<NodeAddress> nodeAddrList = new List<NodeAddress>();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
                nodeAddrList.Add(new NodeAddress(node, 0));
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;
            var srcServerAddress = new NodeAddress(new NodeInfo(sourceNode), 88);
            var srcClientAddress = new NodeAddress(new NodeInfo(sourceNode), 0);
            var targetServerAddress = new NodeAddress(new NodeInfo(targetNode), 88);
            var targetClientAddress = new NodeAddress(new NodeInfo(targetNode), 0);

            List<List<RpcMessage>> msgList = new List<List<RpcMessage>>();
            List<RpcServer> serverList = new List<RpcServer>();
            foreach(var node in nodeAddrList)
            {
                var svrAddr = new NodeAddress(node.Node, 88);
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }


            var logPrefix = Guid.NewGuid().ToString();
            var ledgerLogger = new FilePaxosCommitedDecreeLog(logPrefix + "logger.log");
            var proposerNote = new ProposerNote(ledgerLogger);
            var proposeManager = new ProposeManager(proposerNote.GetMaximumCommittedDecreeNo() + 1);

            var rpcClient = new RpcClient(srcClientAddress);
            // collectlastvote2,
            //  all response lastvote has no voted decree
            //  the decree for voting should be the proposed decree
            {
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent = "test1";

                var proposer = new ProposerRole(
                    cluster.Members[0],
                    cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 1;    // collectlastvote2 will begin a new ballot no

                var collectLastVoteTask = proposer.CollectLastVote(
                    new PaxosDecree(){Content = decreeContent},
                    1);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }

                // 1.1 first last vote message(with null vote) will not change state
                for (int i = 1; i < 5; i++)
                {
                    var lastVote = CreateLastVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 2/*ballotNo*/,
                        0/*votedBallotNo*/, null/*votedDecree*/);
                    await proposer.DeliverLastVoteMessage(lastVote);
                    if ( i >= 3)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                    }
                    Assert.AreEqual(propose.LastVoteMessages.Count, i);

                    var returnedLastVote = propose.LastVoteMessages[i-1];
                    VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 2/*ballotNo*/, 0/*votedBallotNo*/, null/*votedContent*/);
                    VerifyPropose(propose, 2/*lastTriedBallot*/, ProposeState.QueryLastVote, decreeContent);
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                var collectLastVoteResult = await collectLastVoteTask;
                var nextAction = await collectLastVoteResult.OngoingPropose.GetNextAction();
                Assert.AreEqual(nextAction, Propose.NextAction.BeginBallot);

                proposeManager.Reset();
            }

            // collectlastvote2,
            //  some response lastvote has voted decree
            //  the decree for voting should be the decree returned with lastvote
            {
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent = "test1";
                string decreeContent1 = "test0";

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3;    // collectlastvote2 will begin a new ballot no

                var collectLastVoteTask = proposer.CollectLastVote(
                    new PaxosDecree() { Content = decreeContent },
                    1);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }

                // 1.1 first last vote message(with null vote) will not change state
                for (int i = 1; i < 5; i++)
                {
                    LastVoteMessage lastVote = null;
                    if (i == 3)
                    {
                        lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            3/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                    }
                    else if (i == 2)
                    {
                        lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            2/*votedBallotNo*/, decreeContent/*votedDecree*/);
                    }
                    else
                    {
                        lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            0/*votedBallotNo*/, null/*votedDecree*/);
                    }

                    await proposer.DeliverLastVoteMessage(lastVote);
                    if (i >= 3)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                    }
                    Assert.AreEqual(propose.LastVoteMessages.Count, i);

                    var returnedLastVote = propose.LastVoteMessages[i - 1];

                    if (i == 3)
                    {
                        VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 3/*votedBallotNo*/, decreeContent1/*votedContent*/);
                    }
                    else if (i == 2)
                    {
                        VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 2/*votedBallotNo*/, decreeContent/*votedContent*/);
                    }
                    else
                    {
                        VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 0/*votedBallotNo*/, null/*votedContent*/);
                    }

                    if (i <=2)
                    {
                        VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.QueryLastVote, decreeContent);
                    }
                    else
                    {
                        VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.QueryLastVote, decreeContent1);
                    }
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                var collectLastVoteResult = await collectLastVoteTask;
                var nextAction = await collectLastVoteResult.OngoingPropose.GetNextAction();
                Assert.AreEqual(nextAction, Propose.NextAction.BeginBallot);

                proposeManager.Reset();
            }

            // collectlastvote2,
            //  lastvote indicate committed will return commit result immediately
            // TODO: calculate the result dynamically calculating the messages collected in propose
            {
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent = "test1";
                string decreeContent1 = "test0";

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3;    // collectlastvote2 will begin a new ballot no

                var collectLastVoteTask = proposer.CollectLastVote(
                    new PaxosDecree() { Content = decreeContent },
                    1);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }

                for (int i = 1; i < 5; i++)
                {
                    LastVoteMessage lastVote = null;
                    if (i == 2)
                    {
                        lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            3/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                        lastVote.Commited = true;
                    }
                    else
                    {
                        lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            0/*votedBallotNo*/, null/*votedDecree*/);
                    }

                    await proposer.DeliverLastVoteMessage(lastVote);
                    if (i >= 2)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, 2);
                        //if (i > 2)
                        //{
                         //   var returnedLastVote = propose.LastVoteMessages[i - 1];
                         //   VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 0/*votedBallotNo*/, null/*votedContent*/);
                       // }

                        VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.Commited, decreeContent1);

                        Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 1);
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                        Assert.AreEqual(propose.LastVoteMessages.Count, i);

                        var returnedLastVote = propose.LastVoteMessages[i - 1];

                        VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 0/*votedBallotNo*/, null/*votedContent*/);

                        VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.QueryLastVote, decreeContent);

                        Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                    }
                }
                var collectLastVoteResult = await collectLastVoteTask;
                var nextAction = await collectLastVoteResult.OngoingPropose.GetNextAction();
                Assert.AreEqual(nextAction, Propose.NextAction.None);
                Assert.IsTrue(collectLastVoteResult.IsCommitted);
                Assert.AreEqual(collectLastVoteResult.OngoingPropose.GetCommittedDecree().Content, decreeContent1);

                proposeManager.Reset();
            }

            // collectlastvote2,
            //  state message will udpate the nextballotno, and need to collectlastvote2 again
            // TODO: calculate the result dynamically calculating the messages collected in propose
            {
                proposerNote.Clear();
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent = "test1";
                string decreeContent1 = "test0";

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3;    // collectlastvote2 will begin a new ballot no

                var collectLastVoteTask = proposer.CollectLastVote(
                    new PaxosDecree() { Content = decreeContent },
                    1);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }

                for (int i = 1; i < 5; i++)
                {
                    if (i == 2)
                    {
                        var stateMsg = CreateStaleMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            5/*nextBallotNo*/);
                        await proposer.DeliverStaleBallotMessage(stateMsg);
                    }
                    else
                    {
                        var lastVote = CreateLastVoteMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 4/*ballotNo*/,
                            2/*votedBallotNo*/, decreeContent1/*votedDecree*/);
                        await proposer.DeliverLastVoteMessage(lastVote);
                    }

                    if (i >= 2)
                    {
                        Assert.IsTrue(collectLastVoteTask.IsCompleted);
                        break;
                    }
                    else
                    {
                        Assert.IsFalse(collectLastVoteTask.IsCompleted);
                    }
                    Assert.AreEqual(propose.LastVoteMessages.Count, i);

                    var returnedLastVote = propose.LastVoteMessages[i - 1];

                    VerifyLastVoteMessage(returnedLastVote, 1/*decreeNo*/, 4/*ballotNo*/, 2/*votedBallotNo*/, decreeContent1/*votedContent*/);

                    VerifyPropose(propose, 4/*lastTriedBallot*/, ProposeState.QueryLastVote, decreeContent1);
                    Assert.IsTrue(proposerNote.GetCommittedDecreeCount() == 0);
                }
                var collectLastVoteResult = await collectLastVoteTask;
                var nextAction = await collectLastVoteResult.OngoingPropose.GetNextAction();
                Assert.AreEqual(nextAction, Propose.NextAction.CollectLastVote);
                Assert.AreEqual(collectLastVoteResult.OngoingPropose.GetNextBallot(), (ulong)5);

                proposeManager.Reset();
            }

            // collectlastvote2,
            //  state lastvote message will be abandoned
            {
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent = "test1";

                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3;    // collectlastvote2 will begin a new ballot no

                var collectLastVoteTask = proposer.CollectLastVote(
                    new PaxosDecree() { Content = decreeContent },
                    1);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var queryLastVote = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(queryLastVote);
                    msgList[i].Clear();
                }

                LastVoteMessage lastVote = null;

                // state last vote should be abandoned
                lastVote = CreateLastVoteMessage(
                    cluster.Members[1].Name, cluster.Members[0].Name,
                    1/*decreeNo*/, 3/*ballotNo*/,
                    0/*votedBallotNo*/, null/*votedDecree*/);
                await proposer.DeliverLastVoteMessage(lastVote);
                Assert.AreEqual(propose.LastVoteMessages.Count, 0);

                // only stale message and last vote message will be accepted.
                // vote will not be accept at this state
                var votemsg = CreateVoteMessage(
                    cluster.Members[1].Name, cluster.Members[0].Name,
                    1/*decreeNo*/, 4/*ballotNo*/);
                await proposer.DeliverVoteMessage(votemsg);
                // nothing changed
                Assert.AreEqual(propose.VotedMessages.Count, 0);
                Assert.AreEqual(propose.State, ProposeState.QueryLastVote);

                proposeManager.Reset();
            }


            foreach (var rpcserver in serverList)
            {
                await rpcserver.Stop();
            }

            networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
            msgList = new List<List<RpcMessage>>();
            serverList = new List<RpcServer>();
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
            rpcClient = new RpcClient(srcClientAddress);

            // 2. State: BeginNewBallot
            // 3 vote pass
            {
                proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent1 = "test0";
                string decreeContent = "test1";

                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                propose.PrepareNewBallot(new PaxosDecree()
                {
                    Content = decreeContent
                });
                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3; // decreeNo, ballotNo

                LastVoteMessage lastVoteMessage = null;
                // build last vote messages
                for (int i = 1; i < 5; i++)
                {
                    lastVoteMessage = CreateLastVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 3/*ballotNo*/,
                        (ulong)(i != 2 ? 0 : 1)/*votedBallotNo*/,
                        i != 2 ? null : decreeContent1);
                    propose.LastVoteMessages.Add(lastVoteMessage);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 4);

                var beginBallotTask = proposer.BeginNewBallot(1/*decreeNo*/, 3/*ballotNo*/);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var beginBallot = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallot);
                    msgList[i].Clear();
                }

                for (int i = 1; i < 5; i++)
                {
                    var voteMsg = CreateVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 3/*ballotNo*/);
                    await proposer.DeliverVoteMessage(voteMsg);
                    Assert.AreEqual(propose.VotedMessages.Count, i);
                    if (i < 3)
                    {
                        Assert.IsFalse(beginBallotTask.IsCompleted);
                    }
                    else
                    {
                        Assert.IsTrue(beginBallotTask.IsCompleted);
                    }
                }
                var beginBallotResult = await beginBallotTask;
                var nextAction = await beginBallotResult.OngoingPropose.GetNextAction();

                Assert.AreEqual(nextAction, Propose.NextAction.Commit);
                Assert.AreEqual(propose.Decree.Content, decreeContent1);
            }

            // staleballotmessage will reset the state to query last vote
            {
                proposerNote.Clear();
                proposeManager.Reset();
                string decreeContent1 = "test0";
                string decreeContent = "test1";
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                propose.PrepareNewBallot(new PaxosDecree()
                {
                    Content = decreeContent
                });
                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3; // decreeNo, ballotNo

                LastVoteMessage lastVoteMessage = null;
                // build last vote messages
                for (int i = 1; i < 5; i++)
                {
                    lastVoteMessage = CreateLastVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 3/*ballotNo*/,
                        (ulong)(i != 2 ? 0 : 1)/*votedBallotNo*/,
                        i != 2 ? null : decreeContent1);
                    propose.LastVoteMessages.Add(lastVoteMessage);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 4);

                var beginBallotTask = proposer.BeginNewBallot(1/*decreeNo*/, 3/*ballotNo*/);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var beginBallot = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallot);
                    msgList[i].Clear();
                }

                for (int i = 1; i < 5; i++)
                {
                    if (i == 2)
                    {
                        var staleMsg = CreateStaleMessage(
                            cluster.Members[i].Name, cluster.Members[0].Name,
                            1/*decreeNo*/, 3/*ballotNo*/,
                            4/*nextBallotNo*/);
                        await proposer.DeliverStaleBallotMessage(staleMsg);
                        // every thing will be reset, and new query last vote message will be send
                        break;
                    }
                    var voteMsg = CreateVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 3/*ballotNo*/);
                    await proposer.DeliverVoteMessage(voteMsg);
                    Assert.AreEqual(propose.VotedMessages.Count, i);
                    Assert.IsFalse(beginBallotTask.IsCompleted);
                }

                Assert.IsTrue(beginBallotTask.IsCompleted);
                var beginBallotResult = await beginBallotTask;
                var nextAction = await beginBallotResult.OngoingPropose.GetNextAction();
                Assert.AreEqual(nextAction, Propose.NextAction.CollectLastVote);
                VerifyPropose(propose, 3, ProposeState.BeginNewBallot, decreeContent1);
                Assert.AreEqual(propose.LastVoteMessages.Count, 0);
                Assert.AreEqual(propose.VotedMessages.Count, 1);

                propose.PrepareNewBallot(propose.Decree);
                VerifyPropose(propose, 5, ProposeState.QueryLastVote, decreeContent1);

            }

            // lastvote or state vote  will be abandoned
            {
                proposerNote.Clear();
                proposeManager.Reset();
                var propose = proposeManager.AddPropose(1, (ulong)cluster.Members.Count);
                string decreeContent1 = "test0";
                string decreeContent = "test1";
                propose.PrepareNewBallot(new PaxosDecree()
                {
                    Content = decreeContent
                });
                var proposer = new ProposerRole(
                    cluster.Members[0], cluster, rpcClient,
                    proposerNote,
                    proposeManager);
                propose.LastTriedBallot = 3; // decreeNo, ballotNo

                LastVoteMessage lastVoteMessage = null;
                // build last vote messages
                for (int i = 1; i < 4; i++)
                {
                    lastVoteMessage = CreateLastVoteMessage(
                        cluster.Members[i].Name, cluster.Members[0].Name,
                        1/*decreeNo*/, 3/*ballotNo*/,
                        (ulong)(i != 2 ? 0 : 1)/*votedBallotNo*/,
                        i != 2 ? null : decreeContent1);
                    propose.LastVoteMessages.Add(lastVoteMessage);
                }

                var beginBallotTask = proposer.BeginNewBallot(1/*decreeNo*/, 3/*ballotNo*/);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    while (msgList[i].Count == 0) await Task.Delay(50);
                    var beginBallot = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallot);
                    msgList[i].Clear();
                }

                lastVoteMessage = CreateLastVoteMessage(
                    cluster.Members[4].Name, cluster.Members[0].Name,
                    1/*decreeNo*/, 3/*ballotNo*/,
                    0/*votedBallotNo*/,
                    null);
                await proposer.DeliverLastVoteMessage(lastVoteMessage);
                Assert.AreEqual(propose.LastVoteMessages.Count, 0); // lastvote is abandonded

                var voteMsg = CreateVoteMessage(
                    cluster.Members[1].Name, cluster.Members[0].Name,
                    1/*decreeNo*/, 2/*ballotNo*/);
                await proposer.DeliverVoteMessage(voteMsg);
                Assert.AreEqual(propose.VotedMessages.Count, 0);
            }
        }

        [TestMethod()]
        public async Task StateMachineTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeInfo);
                tableNodeMap[nodeInfo.Name] = node;
            }


            var master = tableNodeMap[cluster.Members[0].Name];
            await master.InstertTable(new ReplicatedTableRequest() { Key = "1", Value = "test1" });
            await master.InstertTable(new ReplicatedTableRequest() { Key = "2", Value = "test2" });
            await master.InstertTable(new ReplicatedTableRequest() { Key = "3", Value = "test3" });
   
            foreach (var node in tableNodeMap)
            {
                node.Value.Dispose();
            }
        }

        [TestMethod()]
        public async Task StateMachinePefTest()
        {
            Dictionary<string, string> _table = new Dictionary<string, string>();
            var start = DateTime.Now;
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new ReplicatedTable.ReplicatedTable(cluster, nodeInfo);
                tableNodeMap[nodeInfo.Name] = node;
            }

            start = DateTime.Now;

            List<Task> taskList = new List<Task>();
            var master = tableNodeMap[cluster.Members[0].Name];
            for (int i = 0; i < 10000; i++)
            {
                var task =  master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                taskList.Add(task);
                if (taskList.Count > 2)
                {
                    await Task.WhenAll(taskList);
                    taskList.Clear();
                }
            }
            await Task.WhenAll(taskList);

            end = DateTime.Now;

            costTime = (end - start).TotalMilliseconds;
            Console.WriteLine("TPS: {0}", 10000 * 1000 / costTime);

            foreach (var node in tableNodeMap)
            {
                node.Value.Dispose();
            }
        }

        [TestMethod()]
        public async Task PaxosPefTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var nodeMap = new Dictionary<string, PaxosNode>();
            foreach (var nodeInfo in cluster.Members)
            {
                var node = new PaxosNode(cluster, nodeInfo);
                nodeMap[nodeInfo.Name] = node;
            }

            bool isParallel = true;
            var start = DateTime.Now;

            var proposer = nodeMap[cluster.Members[0].Name];

            var taskList = new List<Task<Paxos.Request.ProposeResult>>();
            for (int i = 0; i < 10000; i++)
            {
                var decree = new PaxosDecree()
                {
                    Content = "test" + i.ToString()
                };
                var task =  proposer.ProposeDecree(decree, 0/*nextDecreNo*/);
                if (isParallel)
                {
                    taskList.Add(task);
                    if (taskList.Count > 2)
                    {
                        await Task.WhenAll(taskList);
                        taskList.Clear();
                    }
                }
                else
                {
                    await task;
                }
            }

            if (isParallel)
            {
                await Task.WhenAll(taskList);
            }
            var end = DateTime.Now;
            var costTime = (end - start).TotalMilliseconds;

            foreach (var node in nodeMap)
            {
                node.Value.Dispose();
            }
        }

        [TestMethod()]
        public async Task PaxosNetworkPefTest()
        {
            var cluster = new PaxosCluster();
            List<NodeAddress> nodeAddrList = new List<NodeAddress>();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
                nodeAddrList.Add(new NodeAddress(node, 0));
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
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
                for (int round = 0; round < 50000; round++)
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
                        var remoteAddr = new NodeAddress(new NodeInfo(node.Name), 88);
                        if (isParallel)
                        {
                            tasks.Add(rpcClient.SendRequest(remoteAddr, rpcMsg));
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
            lastVote.VoteDecree = votedDecree!= null? new PaxosDecree() { Content = votedDecree } : null;

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
                Assert.IsNull(lastVoteMsg.VoteDecree);
            }
            else
            {
                Assert.IsNotNull(lastVoteMsg.VoteDecree);
                Assert.AreEqual(lastVoteMsg.VoteDecree.Content, votedDecreeContent);
            }
        }

        private void VerifyPropose(Propose propose, ulong lastTriedBallot, ProposeState state, string decreeContent)
        {
            Assert.AreEqual(propose.LastTriedBallot, lastTriedBallot);
            // ongoing decree is decreeContent
            Assert.AreEqual(propose.Decree.Content, decreeContent);
            Assert.AreEqual(propose.State, state);

        }

    }
}

namespace PaxsosTests
{
    class PaxosTests
    {
    }
}
