using Microsoft.VisualStudio.TestTools.UnitTesting;
using Paxos.Network;
using Paxos.Protocol;
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

        [TestMethod()]
        public void VoteRoleTest()
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

            var sourceNode = cluster.Members[0].Name;
            var targetNode = cluster.Members[1].Name;

            var ledger = new Ledger();
            var voterFakeTalker = new FakePaxosNodeTalker(cluster.Members[1].Name);
            var voterNote = new VoterNote();
            // 1. NextBallotMessage
            {
                // 1.1. voter have voted no ballot for a decree
                var voter = new VoterRole(cluster.Members[1], cluster, voterFakeTalker, voterNote, ledger);
                var nextBallotMsg = new NextBallotMessage();
                nextBallotMsg.DecreeNo = 1;
                nextBallotMsg.BallotNo = 1;
                nextBallotMsg.SourceNode = sourceNode;
                nextBallotMsg.TargetNode = targetNode;
                voter.DeliverNextBallotMessage(nextBallotMsg);
                var msgList = voterFakeTalker.GetNodeMessages(sourceNode);
                Assert.AreEqual(msgList.Count, 1);
                var lastVoteMsg = msgList[0] as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.IsNull(lastVoteMsg.VoteDecree);
                Assert.AreEqual(voterNote.NextBallotNo[nextBallotMsg.DecreeNo], (ulong)1);
                Assert.IsFalse(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                // now the NextBalloNo is 1
                // 1.2. New ballot no > 1 will be accepted and got a last vote
                nextBallotMsg.BallotNo = 2;
                voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg = msgList[0] as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)0);
                Assert.IsNull(lastVoteMsg.VoteDecree);
                Assert.AreEqual(voterNote.NextBallotNo[nextBallotMsg.DecreeNo], (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                // now NextBallotNo is 2

                // 1.3. NextBallotNo <= 2 will not be accepted and got a stale ballot message
                voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg = msgList[0] as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)2);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                nextBallotMsg.BallotNo = 1;
                voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                staleBallotMsg = msgList[0] as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);
                Assert.IsFalse(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                //1.4 voter has voted ballot before
                string voteContent = "test1";
                var voteMsg = new VoteMessage();
                voteMsg.DecreeNo = 1;
                voteMsg.BallotNo = 2;
                voteMsg.VoteDecree = new PaxosDecree()
                { Content = voteContent };
                voteMsg.SourceNode = sourceNode;
                voteMsg.TargetNode = targetNode;
                voterNote.VotedMessage.Add(1, voteMsg);

                nextBallotMsg.BallotNo = 3;
                voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg = msgList[0] as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)3);
                Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecree.Content, voteContent);
                Assert.IsFalse(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                //1.5 decree has been committed in ledger
                ledger.CommitedDecrees.Add(1, voteMsg.VoteDecree);
                nextBallotMsg.BallotNo = 2; // do not care about the ballot no for committed decree

                voter.DeliverNextBallotMessage(nextBallotMsg);
                Assert.AreEqual(msgList.Count, 1);
                lastVoteMsg = msgList[0] as LastVoteMessage;
                Assert.IsNotNull(lastVoteMsg);
                Assert.AreEqual(lastVoteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(lastVoteMsg.BallotNo, (ulong)2);
                //Assert.AreEqual(lastVoteMsg.VoteBallotNo, (ulong)2);
                Assert.AreEqual(lastVoteMsg.VoteDecree.Content, voteContent);
                Assert.IsTrue(lastVoteMsg.Commited);
                voterFakeTalker.ClearNodeMessage(sourceNode);

                // cleanup
                ledger.Clear();

            }

            // 2. StartNewBallotMessage
            {
                voterNote.Reset();

                // 2.2 has no NextBallotNo yet
                var voter = new VoterRole(cluster.Members[1], cluster, voterFakeTalker, voterNote, ledger);

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
                voter.DeliverBeginBallotMessage(beginBallotMsg);    // no response
                var msgList = voterFakeTalker.GetNodeMessages(sourceNode);
                Assert.AreEqual(msgList.Count, 0);

                voterNote.NextBallotNo.Add(1, 2); // nextBallotNo = 2, > 1
                voter.DeliverBeginBallotMessage(beginBallotMsg);    // state ballot response
                Assert.AreEqual(msgList.Count, 1);
                var staleBallotMsg = msgList[0] as StaleBallotMessage;
                Assert.IsNotNull(staleBallotMsg);
                Assert.AreEqual(staleBallotMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.BallotNo, (ulong)1);
                Assert.AreEqual(staleBallotMsg.NextBallotNo, (ulong)2);    //nextBallotNo > ballotNo 
                voterFakeTalker.ClearNodeMessage(sourceNode);


                // 2.2 NextBallotNo match
                beginBallotMsg.BallotNo = 2;
                voter.DeliverBeginBallotMessage(beginBallotMsg);    // vote
                Assert.AreEqual(msgList.Count, 1);

                var voteMsg = msgList[0] as VoteMessage;
                Assert.IsNotNull(voteMsg);
                Assert.AreEqual(voteMsg.DecreeNo, (ulong)1);
                Assert.AreEqual(voteMsg.BallotNo, (ulong)2);
                Assert.IsNotNull(voteMsg.VoteDecree);
                Assert.AreEqual(voteMsg.VoteDecree.Content, voteContent);

                voterFakeTalker.ClearNodeMessage(sourceNode);

                // 2.3 Decree committed, no response
                ledger.CommitedDecrees.Add(beginBallotMsg.DecreeNo, voteMsg.VoteDecree);
                beginBallotMsg.BallotNo = 3;
                voter.DeliverBeginBallotMessage(beginBallotMsg);    // vote
                Assert.AreEqual(msgList.Count, 0);
                voterFakeTalker.ClearNodeMessage(sourceNode);
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

            //var sourceNode = cluster.Members[0].Name;
            //var targetNode = cluster.Members[1].Name;

            var proposerFakeTalker = new FakePaxosNodeTalker(cluster.Members[0].Name);
            var ledger = new Ledger();
            var proposerNote = new ProposerNote();
            var nodeMsgList = new List<List<PaxosMessage>>();
            foreach (var node in cluster.Members)
            {
                proposerFakeTalker.BuildNodeMessageList(node.Name);
                nodeMsgList.Add(proposerFakeTalker.GetNodeMessages(node.Name));
            }

            // 1. State: QueryLastVote
            {
                proposerNote.DecreeState.Add(1, new Propose()
                {
                    State = PropserState.QueryLastVote
                });
                string decreeContent1 = "test0";
                string decreeContent = "test1";
                var proposer = new ProposerRole(cluster.Members[0], cluster, proposerFakeTalker, proposerNote, ledger);
                proposerNote.LastTriedBallot.Add(1, 2); // decreeNo, ballotNo
                proposerNote.OngoingPropose.Add(1, new PaxosDecree()
                {
                    Content = decreeContent
                });

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
                foreach(var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
                }
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 1);
                var returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);
                // ongoing decree is decreeContent
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent);
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.QueryLastVote);
                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 0);

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
                foreach (var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
                }
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 2);
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);
                // ongoing decree is decreeContent, not changed
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent);
                // query last vote state
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.QueryLastVote);
                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 0);

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
                foreach (var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
                }
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 2);
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);
                // ongoing decree is decreeContent, not changed
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent);
                // query last vote state
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.QueryLastVote);

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 0);

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
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 3);
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                returnedLastVote = proposerNote.LastVoteMessages[1][2];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);

                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);

                // one message for every nodes other than itself
                Assert.IsTrue(nodeMsgList[0].Count == 0);
                for (int i = 1; i < nodeMsgList.Count; i++)
                {
                    Assert.IsTrue(nodeMsgList[i].Count == 1);
                    var beginBallotMsg = nodeMsgList[i][0] as BeginBallotMessage;
                    Assert.IsNotNull(beginBallotMsg);
                    Assert.AreEqual(beginBallotMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(beginBallotMsg.BallotNo, (ulong)2);
                    Assert.AreEqual(beginBallotMsg.TargetNode, cluster.Members[i].Name);
                    Assert.AreEqual(beginBallotMsg.Decree.Content, decreeContent1);
                    nodeMsgList[i].Clear();
                }

                // ongoing decree now changed decreeContent1 since it's the maximum vote decree
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent1);
                // now the state has changed
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.BeginNewBallot);

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 0);

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
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 3);
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                returnedLastVote = proposerNote.LastVoteMessages[1][2];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);

                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);

                // no more messages
                foreach (var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
                }

                // ongoing decree now changed decreeContent1 since it's the maximum vote decree
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent1);
                // now the state has changed
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.BeginNewBallot);

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 0);


                // 1.6 lastvote with a committed vote will change the state to commit
                proposerNote.Reset();
                proposerNote.DecreeState.Add(1, new Propose()
                {
                    State = PropserState.QueryLastVote
                });
                proposerNote.LastTriedBallot.Add(1, 2); // decreeNo, ballotNo
                proposerNote.OngoingPropose.Add(1, new PaxosDecree()
                {
                    Content = decreeContent
                });

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
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 2); // last vote recorded not change
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);

                // ongoing decree now changed decreeContent since it's the committed decree
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent);
                // now the state has changed
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.Commited);

                Assert.IsTrue(nodeMsgList[0].Count == 0);
                for (int i = 1; i < nodeMsgList.Count; i++)
                {
                    Assert.IsTrue(nodeMsgList[i].Count == 1);
                    var commitDecreeMsg = nodeMsgList[i][0] as SuccessMessage;
                    Assert.IsNotNull(commitDecreeMsg);
                    Assert.AreEqual(commitDecreeMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(commitDecreeMsg.BallotNo, (ulong)2);
                    Assert.AreEqual(commitDecreeMsg.TargetNode, cluster.Members[i].Name);
                    Assert.AreEqual(commitDecreeMsg.Decree.Content, decreeContent);
                    nodeMsgList[i].Clear();
                }

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 1);
                Assert.IsTrue(ledger.CommitedDecrees[1].Content.Equals(decreeContent));

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
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 2); // last vote recorded not change
                returnedLastVote = proposerNote.LastVoteMessages[1][0];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)0);
                Assert.IsNull(returnedLastVote.VoteDecree);
                returnedLastVote = proposerNote.LastVoteMessages[1][1];
                Assert.AreEqual(returnedLastVote.DecreeNo, (ulong)1);
                Assert.AreEqual(returnedLastVote.BallotNo, (ulong)2);
                Assert.AreEqual(returnedLastVote.VoteBallotNo, (ulong)1);
                Assert.IsNotNull(returnedLastVote.VoteDecree);
                Assert.AreEqual(returnedLastVote.VoteDecree.Content, decreeContent1);
                // last tried ballot is 2
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)2);

                // ongoing decree now changed decreeContent since it's the committed decree
                Assert.AreEqual(proposerNote.OngoingPropose[1].Content, decreeContent);
                // now the state has changed
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.Commited);
                for (int i = 0; i < nodeMsgList.Count; i++)
                {
                    Assert.IsTrue(nodeMsgList[i].Count == 0);
                }

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 1);
                Assert.IsTrue(ledger.CommitedDecrees[1].Content.Equals(decreeContent));

                proposerNote.ClearDecree(1);
                await proposer.DeliverLastVoteMessage(lastVote);  // dropped
                for (int i = 0; i < nodeMsgList.Count; i++)
                {
                    Assert.IsTrue(nodeMsgList[i].Count == 0);
                }

                // ledger unchanged
                Assert.IsTrue(ledger.CommitedDecrees.Count == 1);
                Assert.IsTrue(ledger.CommitedDecrees[1].Content.Equals(decreeContent));
            }

            // 2. State: BeginNewBallot
            {
                ledger.Clear();
                proposerNote.Reset();
                proposerNote.DecreeState.Add(1, new Propose()
                {
                    State = PropserState.BeginNewBallot
                });
                string decreeContent1 = "test0";
                string decreeContent = "test1";
                var proposer = new ProposerRole(cluster.Members[0], cluster, proposerFakeTalker, proposerNote,ledger);
                proposerNote.LastTriedBallot.Add(1, 3); // decreeNo, ballotNo
                proposerNote.OngoingPropose.Add(1, new PaxosDecree()
                {
                    Content = decreeContent
                });

                // build last vote messages
                proposerNote.LastVoteMessages.Add(1, new List<LastVoteMessage>());
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
                    proposerNote.LastVoteMessages[1].Add(lastVoteMessage);
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
                foreach (var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
                }
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 2);


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
                Assert.AreEqual(proposerNote.VoteMessages[1].Count, 1);
                foreach (var msgList in nodeMsgList)
                {
                    Assert.IsTrue(msgList.Count == 0);
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
                Assert.AreEqual(proposerNote.VoteMessages[1].Count, 1);

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
                Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.QueryLastVote);
                Assert.AreEqual(proposerNote.LastTriedBallot[1], (ulong)5);
                Assert.AreEqual(proposerNote.LastVoteMessages[1].Count, 0);
                Assert.AreEqual(proposerNote.VoteMessages[1].Count, 0);
                for (int i = 1; i < nodeMsgList.Count; i++)
                {
                    Assert.IsTrue(nodeMsgList[i].Count == 1);
                    var nextBallotMsg = nodeMsgList[i][0] as NextBallotMessage;
                    Assert.IsNotNull(nextBallotMsg);
                    Assert.AreEqual(nextBallotMsg.DecreeNo, (ulong)1);
                    Assert.AreEqual(nextBallotMsg.BallotNo, (ulong)5);
                    Assert.AreEqual(nextBallotMsg.TargetNode, cluster.Members[i].Name);
                    nodeMsgList[i].Clear();
                }


                // build vote state
                proposerNote.DecreeState[1].State = PropserState.BeginNewBallot;
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
                    proposerNote.LastVoteMessages[1].Add(lastVoteMessage);
                }


                proposerNote.LastTriedBallot[1] = 5; // decreeNo, ballotNo
                proposerNote.OngoingPropose[1].Content = decreeContent1;

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
                    Assert.AreEqual(proposerNote.VoteMessages[1].Count, i);
                    if (i == 3)
                    {
                        Assert.IsTrue(nodeMsgList[0].Count == 0);
                        for (int j = 1; j < nodeMsgList.Count; j++)
                        {
                            Assert.IsTrue(nodeMsgList[j].Count == 1);
                            var commitMsg = nodeMsgList[j][0] as SuccessMessage;
                            Assert.IsNotNull(commitMsg);
                            Assert.AreEqual(commitMsg.DecreeNo, (ulong)1);
                            Assert.AreEqual(commitMsg.BallotNo, (ulong)5);
                            Assert.AreEqual(commitMsg.TargetNode, cluster.Members[j].Name);
                            Assert.AreEqual(commitMsg.Decree.Content, decreeContent1);
                            nodeMsgList[j].Clear();
                        }
                        Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.Commited);
                    }
                    else
                    {
                        foreach (var msgList in nodeMsgList)
                        {
                            Assert.IsTrue(msgList.Count == 0);
                        }
                        Assert.AreEqual(proposerNote.DecreeState[1].State, PropserState.BeginNewBallot);
                    }
                }
            }
        }

    }
}

namespace PaxsosTests
{
    class PaxosTests
    {
    }
}
