using Microsoft.VisualStudio.TestTools.UnitTesting;
using Paxos.Message;
using Paxos.Network;
using Paxos.Notebook;
using Paxos.Protocol;
using Paxos.Persistence;
using Paxos.Node;
using Paxos.Request;
using Paxos.Rpc;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
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

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            var nodeMap = new Dictionary<string, PaxosNode>();
            var network = new TestNetwork(networkInfr);
            network.CreateNetworkMap(cluster.Members);

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
            await network.WaitUntillAllReceivedMessageConsumed();
            readReslut = await proposer.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);

            readReslut = await proposer2.ReadDecree(result.DecreeNo);
            Assert.IsTrue(readReslut.IsFound);
            Assert.IsTrue(readReslut.Decree.Content.Equals("test3"));
            Assert.IsTrue(readReslut.MaxDecreeNo == 3);
        }

        [TestMethod()]
        public async Task VoteRoleTest()
        {
            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo();
                node.Name = "Node" + i.ToString();
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
            var network = new TestNetwork(networkInfr);
            network.CreateNetworkMap(cluster.Members);
            
            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;
            var srcServerAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = sourceNode },
                Port = 88
            };
            var srcClientAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = sourceNode },
                Port = 0
            };
            var targetServerAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = targetNode },
                Port = 88
            };
            var targetClientAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = targetNode },
                Port = 0
            };

            var ledger = new Ledger();

            var persistenter = new MemoryPaxosNotePersistent();
            var voterNote = new VoterNote(persistenter);
            var decreeLockManager = new DecreeLockManager();

            var msgList = new List<RpcMessage>();
            // 1. NextBallotMessage
            {
                // 1.1. voter have voted no ballot for a decree
                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                var rpcClient = new RpcClient(targetClientAddress);
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, decreeLockManager, voterNote, ledger);
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
                voteMsg.VoteDecree = new PaxosDecree()
                { Content = voteContent };
                voteMsg.SourceNode = sourceNode;
                voteMsg.TargetNode = targetNode;

                await voterNote.UpdateLastVote(1, 2, voteMsg);

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
                await ledger.CommitDecree(1, voteMsg.VoteDecree);
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
                ledger.Clear();

            }

            // 2. StartNewBallotMessage
            {
                voterNote.Reset();

                networkInfr = new TestNetworkInfr();
                NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
                network = new TestNetwork(networkInfr);
                network.CreateNetworkMap(cluster.Members);


                var rpcServer = new RpcServer(srcServerAddress);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgList));
                await rpcServer.Start();

                // 2.2 has no NextBallotNo yet
                var rpcClient = new RpcClient(targetClientAddress);
                var voter = new VoterRole(cluster.Members[1], cluster, rpcClient, decreeLockManager, voterNote, ledger);

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
                Assert.IsNotNull(voteMsg.VoteDecree);
                Assert.AreEqual(voteMsg.VoteDecree.Content, voteContent);

                msgList.Clear();

                // 2.3 Decree committed, no response
                await ledger.CommitDecree(beginBallotMsg.DecreeNo, voteMsg.VoteDecree);
                beginBallotMsg.BallotNo = 3;
                await voter.DeliverBeginBallotMessage(beginBallotMsg);    // vote
                Assert.AreEqual(msgList.Count, 0);
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
                var node = new NodeInfo();
                node.Name = "Node" + i.ToString();
                cluster.Members.Add(node);
                nodeAddrList.Add(new NodeAddress()
                {
                    Node = node,
                    Port = 0
                });
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
            var network = new TestNetwork(networkInfr);
            network.CreateNetworkMap(cluster.Members);

            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;
            var srcServerAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = sourceNode },
                Port = 88
            };
            var srcClientAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = sourceNode },
                Port = 0
            };
            var targetServerAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = targetNode },
                Port = 88
            };
            var targetClientAddress = new NodeAddress()
            {
                Node = new NodeInfo()
                { Name = targetNode },
                Port = 0
            };

            List<List<RpcMessage>> msgList = new List<List<RpcMessage>>();
            List<RpcServer> serverList = new List<RpcServer>();
            foreach(var node in nodeAddrList)
            {
                var svrAddr = new NodeAddress()
                {
                    Node = node.Node,
                    Port = 88
                };
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }


            var ledger = new Ledger();
            var proposerNote = new ProposerNote(ledger);
            var decreeLockManager = new DecreeLockManager();

            // 1. State: QueryLastVote
            {

                var propose = proposerNote.AddPropose(1);
                propose.State = PropserState.QueryLastVote;
                string decreeContent1 = "test0";
                string decreeContent = "test1";

                var rpcClient = new RpcClient(srcClientAddress);
                var proposer = new ProposerRole(cluster.Members[0], cluster, rpcClient, decreeLockManager, proposerNote, ledger);
                propose.LastTriedBallot = 2;
                propose.OngoingDecree = new PaxosDecree()
                {
                    Content = decreeContent
                };

                // 1.1 first last vote message(with null vote) will not change state
                var lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[1].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 0; // never vote
                lastVote.VoteDecree = null;
                await proposer.DeliverLastVoteMessage(lastVote);
                // none message for all nodes
                for(int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 1);
                var returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);
                // ongoing decree is decreeContent
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent);
                Assert.AreEqual(propose.State, PropserState.QueryLastVote);
                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 0);

                // 1.2 second last vote message(with vote) will not change state
                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[2].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 1;
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent1
                };
                await proposer.DeliverLastVoteMessage(lastVote);
                // none message for all nodes
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 2);
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);
                // ongoing decree is decreeContent, not changed
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent);
                // query last vote state
                Assert.AreEqual(propose.State, PropserState.QueryLastVote);
                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 0);

                // 1.3 one stale last vote message(ballot no mot match) is dropped, not change state
                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[3].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 1;
                lastVote.VoteBallotNo = 0; // never vote
                lastVote.VoteDecree = null;
                await proposer.DeliverLastVoteMessage(lastVote);
                // none message for all nodes
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 2);
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);
                // ongoing decree is decreeContent, not changed
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent);
                // query last vote state
                Assert.AreEqual(propose.State, PropserState.QueryLastVote);

                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 0);

                //1.4 third last vote message(with different vote) change the state to beginnewballot
                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[3].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 1;
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent1
                };
                await proposer.DeliverLastVoteMessage(lastVote);
                Assert.AreEqual(propose.LastVoteMessages.Count, 3);
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                returnedLastVote = propose.LastVoteMessages[2];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);

                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);

                // one message for every nodes other than itself
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 1);
                    var beginBallotMsg = CreatePaxosMessage(msgList[i][0]) as BeginBallotMessage;
                    Assert.IsNotNull(beginBallotMsg);
                    Assert.AreEqual(beginBallotMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(beginBallotMsg.BallotNo, (ulong)2);
                    Assert.AreEqual(beginBallotMsg.TargetNode, cluster.Members[i].Name);
                    Assert.AreEqual(beginBallotMsg.Decree.Content, decreeContent1);
                    msgList[i].Clear();
                }

                // ongoing decree now changed decreeContent1 since it's the maximum vote decree
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent1);
                // now the state has changed
                Assert.AreEqual(propose.State, PropserState.BeginNewBallot);

                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 0);

                //1.5 fourth last vote, this message will be dropped since state already changed
                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[4].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 1;
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent1
                };
                await proposer.DeliverLastVoteMessage(lastVote);
                Assert.AreEqual(propose.LastVoteMessages.Count, 3);
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                returnedLastVote = propose.LastVoteMessages[2];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);

                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);

                // no more messages
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }


                // ongoing decree now changed decreeContent1 since it's the maximum vote decree
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent1);
                // now the state has changed
                Assert.AreEqual(propose.State, PropserState.BeginNewBallot);

                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 0);


                // 1.6 lastvote with a committed vote will change the state to commit
                proposerNote.Reset();
                propose = proposerNote.AddPropose(1);
                propose.State = PropserState.QueryLastVote;
                propose.LastTriedBallot = 2; // decreeNo, ballotNo
                propose.OngoingDecree = new PaxosDecree()
                {
                    Content = decreeContent
                };

                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[1].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 0; // never vote
                lastVote.VoteDecree = null;
                await proposer.DeliverLastVoteMessage(lastVote);

                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[2].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 1;
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent1
                };
                await proposer.DeliverLastVoteMessage(lastVote);


                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[4].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.Commited = true; 
                lastVote.VoteBallotNo = 0;  // not care
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent
                };
                await proposer.DeliverLastVoteMessage(lastVote);
                Assert.AreEqual(propose.LastVoteMessages.Count, 2); // last vote recorded not change
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);

                // ongoing decree now changed decreeContent since it's the committed decree
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent);
                // now the state has changed
                Assert.AreEqual(propose.State, PropserState.Commited);

                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 1);
                    var commitDecreeMsg = CreatePaxosMessage(msgList[i][0]) as SuccessMessage;
                    Assert.IsNotNull(commitDecreeMsg);
                    Assert.AreEqual(commitDecreeMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(commitDecreeMsg.BallotNo, (ulong)2);
                    Assert.AreEqual(commitDecreeMsg.TargetNode, cluster.Members[i].Name);
                    Assert.AreEqual(commitDecreeMsg.Decree.Content, decreeContent);
                    msgList[i].Clear();
                }


                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 1);
                Assert.IsTrue(ledger.GetCommittedDecree(1).Content.Equals(decreeContent));

                //1.7 proposer's ledger has committed decree, all the messages should be abandoned
                // based on above test, new lastvote will be dropped, nothing changed
                //proposerNote.ClearDecree(1);
                lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[3].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 2;
                lastVote.VoteBallotNo = 1;
                lastVote.VoteDecree = new PaxosDecree()
                {
                    Content = decreeContent1
                };
                await proposer.DeliverLastVoteMessage(lastVote);  // dropped
                Assert.AreEqual(propose.LastVoteMessages.Count, 2); // last vote recorded not change
                returnedLastVote = propose.LastVoteMessages[0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = propose.LastVoteMessages[1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(propose.LastTriedBallot, (ulong)2);

                // ongoing decree now changed decreeContent since it's the committed decree
                Assert.AreEqual(propose.OngoingDecree.Content, decreeContent);
                // now the state has changed
                Assert.AreEqual(propose.State, PropserState.Commited);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }


                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 1);
                Assert.IsTrue(ledger.GetCommittedDecree(1).Content.Equals(decreeContent));

                proposerNote.ClearDecree(1);
                await proposer.DeliverLastVoteMessage(lastVote);  // dropped
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }

                // ledger unchanged
                Assert.IsTrue(ledger.GetCommittedDecreeCount() == 1);
                Assert.IsTrue(ledger.GetCommittedDecree(1).Content.Equals(decreeContent));
            }

            foreach(var rpcserver in serverList)
            {
                rpcserver.Stop = true;
            }

            networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));
            network = new TestNetwork(networkInfr);
            network.CreateNetworkMap(cluster.Members);

            msgList = new List<List<RpcMessage>>();
            serverList = new List<RpcServer>();
            foreach (var node in nodeAddrList)
            {
                var svrAddr = new NodeAddress()
                {
                    Node = node.Node,
                    Port = 88
                };
                var msgs = new List<RpcMessage>();
                msgList.Add(msgs);
                var rpcServer = new RpcServer(svrAddr);
                rpcServer.RegisterRequestHandler(new TestRpcRequestHandler(msgs));
                await rpcServer.Start();
                serverList.Add(rpcServer);
            }
            // 2. State: BeginNewBallot
            {
                ledger.Clear();
                proposerNote.Reset();
                var propose = proposerNote.AddPropose(1);
                propose.State = PropserState.BeginNewBallot;
                string decreeContent1 = "test0";
                string decreeContent = "test1";
                var rpcClient = new RpcClient(srcClientAddress);
                var proposer = new ProposerRole(cluster.Members[0], cluster, rpcClient, decreeLockManager, proposerNote, ledger);
                propose.LastTriedBallot = 3; // decreeNo, ballotNo
                propose.OngoingDecree = new PaxosDecree()
                {
                    Content = decreeContent
                };

                // build last vote messages
                for (int i = 1; i < 3; i++)
                {
                    var lastVoteMessage = new LastVoteMessage()
                    {
                        SourceNode = cluster.Members[i].Name,
                        TargetNode = cluster.Members[0].Name,
                        DecreeNo = 1,
                        BallotNo = 3,
                        VoteBallotNo = 0,
                        VoteDecree = null
                    };
                    if (i == 2)
                    {
                        lastVoteMessage.VoteBallotNo = 1;
                        lastVoteMessage.VoteDecree = new PaxosDecree()
                        {
                            Content = decreeContent
                        };
                    }
                    propose.LastVoteMessages.Add(lastVoteMessage);
                }

                // last vote will be dropped directly
                var lastVote = new LastVoteMessage();
                lastVote.SourceNode = cluster.Members[4].Name;
                lastVote.TargetNode = cluster.Members[0].Name;
                lastVote.DecreeNo = 1;
                lastVote.BallotNo = 3;
                lastVote.VoteBallotNo = 0; // never vote
                lastVote.VoteDecree = null;
                await proposer.DeliverLastVoteMessage(lastVote);
                // none message for all nodes
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    var nodeAddr = nodeAddrList[i];
                    Assert.IsTrue(msgList[i].Count == 0);
                }
                Assert.AreEqual(propose.LastVoteMessages.Count, 2);


                // first vote
                var voteMsg = new VoteMessage()
                {
                    DecreeNo = 1,
                    BallotNo = 3,
                    SourceNode = cluster.Members[1].Name,
                    TargetNode = cluster.Members[0].Name,
                    VoteDecree = new PaxosDecree()
                    { Content = decreeContent }
                };
                await proposer.DeliverVoteMessage(voteMsg);
                Assert.AreEqual(propose.VotedMessages.Count, 1);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 0);
                }

                // stale vote will be dropped
                voteMsg = new VoteMessage()
                {
                    DecreeNo = 1,
                    BallotNo = 1,
                    SourceNode = cluster.Members[2].Name,
                    TargetNode = cluster.Members[0].Name,
                    VoteDecree = new PaxosDecree()
                    { Content = decreeContent }
                };
                await proposer.DeliverVoteMessage(voteMsg);
                Assert.AreEqual(propose.VotedMessages.Count, 1);

                // stale ballot message
                var staleMsg = new StaleBallotMessage()
                {
                    DecreeNo = 1,
                    BallotNo = 3,
                    SourceNode = cluster.Members[4].Name,
                    TargetNode = cluster.Members[0].Name,
                    NextBallotNo = 4
                };
                await proposer.DeliverStaleBallotMessage(staleMsg);
                // every thing will be reset, and new query last vote message will be send
                Assert.AreEqual(propose.State, PropserState.QueryLastVote);
                Assert.AreEqual(propose.LastTriedBallot, (ulong)5);
                Assert.AreEqual(propose.LastVoteMessages.Count, 0);
                Assert.AreEqual(propose.VotedMessages.Count, 0);
                for (int i = 1; i < nodeAddrList.Count; i++)
                {
                    Assert.IsTrue(msgList[i].Count == 1);
                    var nextBallotMsg = CreatePaxosMessage(msgList[i][0]) as NextBallotMessage;
                    Assert.IsNotNull(nextBallotMsg);
                    Assert.AreEqual(nextBallotMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(nextBallotMsg.BallotNo, (ulong)5);
                    Assert.AreEqual(nextBallotMsg.TargetNode, cluster.Members[i].Name);
                    msgList[i].Clear();
                }


                // build vote state
                propose.State = PropserState.BeginNewBallot;
                // build last vote messages
                for (int i = 1; i < 4; i++)
                {
                    var lastVoteMessage = new LastVoteMessage()
                    {
                        SourceNode = cluster.Members[i].Name,
                        TargetNode = cluster.Members[0].Name,
                        DecreeNo = 1,
                        BallotNo = 3,
                        VoteBallotNo = 0,
                        VoteDecree = null
                    };
                    if (i == 2)
                    {
                        lastVoteMessage.VoteBallotNo = 1;
                        lastVoteMessage.VoteDecree = new PaxosDecree()
                        {
                            Content = decreeContent
                        };
                    }
                    if (i == 4)
                    {
                        lastVoteMessage.VoteBallotNo = 4;
                        lastVoteMessage.VoteDecree = new PaxosDecree()
                        {
                            Content = decreeContent1
                        };
                    }
                    propose.LastVoteMessages.Add(lastVoteMessage);
                }


                propose.LastTriedBallot = 5; // decreeNo, ballotNo
                propose.OngoingDecree.Content = decreeContent1;

                // first vote
                for (int i = 1; i < 4; i++)
                {
                    voteMsg = new VoteMessage()
                    {
                        DecreeNo = 1,
                        BallotNo = 5,
                        SourceNode = cluster.Members[i].Name,
                        TargetNode = cluster.Members[0].Name,
                        VoteDecree = new PaxosDecree()
                        { Content = decreeContent1 }
                    };
                    await proposer.DeliverVoteMessage(voteMsg);
                    Assert.AreEqual(propose.VotedMessages.Count, i);
                    if (i == 3)
                    {
                        for (int j = 1; i < nodeAddrList.Count; i++)
                        {
                            Assert.IsTrue(msgList[j].Count == 1);
                            var commitMsg = CreatePaxosMessage(msgList[j][0]) as SuccessMessage;
                            Assert.IsNotNull(commitMsg);
                            Assert.AreEqual(commitMsg.DecreeNo, (ulong)1);
                            Assert.AreEqual(commitMsg.BallotNo, (ulong)5);
                            Assert.AreEqual(commitMsg.TargetNode, cluster.Members[j].Name);
                            Assert.AreEqual(commitMsg.Decree.Content, decreeContent1);
                            msgList[j].Clear();
                        }
                        Assert.AreEqual(propose.State, PropserState.Commited);
                    }
                    else
                    {
                        for (int j = 1; i < nodeAddrList.Count; i++)
                        {
                            Assert.IsTrue(msgList[j].Count == 0);
                        }

                        Assert.AreEqual(propose.State, PropserState.BeginNewBallot);
                    }
                }
            }
        }

        private PaxosMessage CreatePaxosMessage(RpcMessage rpcMessage)
        {
            var paxosRpcMessage = PaxosRpcMessageFactory.CreatePaxosRpcMessage(rpcMessage);
            var paxosMessage = PaxosMessageFactory.CreatePaxosMessage(paxosRpcMessage);
            return paxosMessage;
        }
    }
}

namespace PaxsosTests
{
    class PaxosTests
    {
    }
}
