using PineRSL.Common;
using PineRSL.Network;
using PineRSL.Paxos.Node;
using PineRSL.Paxos.Protocol;
using PineRSL.Paxos.Request;
using PineRSL.ReplicatedTable;
using PineRSL.Tests;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PineRSL
{
    class Program
    {
        static void LogStat(string name, Statistic stat)
        {
            Console.WriteLine("{0}: avg={1}, min={2}, max={3}", name, stat.Avg, stat.Min, stat.Max);
        }

        static async Task Main(string[] args)
        {
            int maxWorker = 0;
            int maxIocp = 0;

            string randomstr = new string('t', 1024 * 5);
            var randomData = Encoding.UTF8.GetBytes(randomstr);
            var prefixData = Encoding.UTF8.GetBytes("test");

            ThreadPool.GetAvailableThreads(out maxWorker, out maxIocp);
            ThreadPool.GetMinThreads(out maxWorker, out maxIocp);
            var currentThreadCount = ThreadPool.ThreadCount;
            if (ThreadPool.SetMinThreads(maxWorker * 40, maxIocp * 3))
            {
                Console.WriteLine("success increase min threads");
                ThreadPool.GetAvailableThreads(out maxWorker, out maxIocp);
                currentThreadCount = ThreadPool.ThreadCount;
            }

            var cluster = new PaxosCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("Node" + i.ToString());
                cluster.Members.Add(node);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr));

            if (false)
            {

                var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
                foreach (var nodeInfo in cluster.Members)
                {
                    var node = new ReplicatedTable.ReplicatedTable(cluster, nodeInfo);
                    tableNodeMap[nodeInfo.Name] = node;
                }

                var start = DateTime.Now;

                List<Task> taskList = new List<Task>();
                var master = tableNodeMap[cluster.Members[0].Name];
                for (int i = 0; i < 10000000; i++)
                {
                    var task = master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                    taskList.Add(task);
                    if (taskList.Count > 20000)
                    {
                        await Task.WhenAll(taskList);
                        taskList.Clear();
                        var end = DateTime.Now;
                        var costTime = (end - start).TotalMilliseconds;
                        Console.WriteLine("Request {0}, TPS: {1}", i, i * 1000 / costTime);
                    }
                }
                await Task.WhenAll(taskList);
                foreach (var node in tableNodeMap)
                {
                    node.Value.Dispose();
                }

            }
            else
            {
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
                currentThreadCount = ThreadPool.ThreadCount;

                Statistic collectLastVoteTime = new Statistic();
                Statistic voteTime = new Statistic();
                Statistic commitTime = new Statistic();
                Statistic getProposeTime = new Statistic();
                Statistic getProposeLockTime = new Statistic();
                Statistic prepareNewBallotTime = new Statistic();
                Statistic broadcaseQueryLastVoteTime = new Statistic();

                Statistic proposeDecreeTime = new Statistic();
                Statistic taskCreateTime = new Statistic();


                DateTime beginWait = DateTime.Now;
                var taskList = new List<Task>();
                var reqList = new List<int>();
                for (UInt64 i = 0; i < 1000000000; i++)
                {
                    //var data = new byte[prefixData.Length + sizeof(int) + randomData.Length];
                    //Buffer.BlockCopy(prefixData, 0, data, 0, prefixData.Length);
                    //Buffer.BlockCopy(BitConverter.GetBytes(i), 0, data, prefixData.Length, sizeof(int));
                    var decree = new PaxosDecree()
                    {
                        Data = randomData//data
                        //Content = "test" + i.ToString() + randomstr
                    };
                    var beforeCreateTaskTime = DateTime.Now;
                    var task = Task.Run(async () =>
                    {
                        var begin = DateTime.Now;
                        var result = await proposer.ProposeDecree(decree, 0/*nextDecreNo*/);
                        //var proposeTime = DateTime.Now - begin;
                        //proposeDecreeTime.Accumulate(proposeTime.TotalMilliseconds);
                        //collectLastVoteTime.Accumulate(result.CollectLastVoteTimeInMs.TotalMilliseconds);
                        //voteTime.Accumulate(result.VoteTimeInMs.TotalMilliseconds);
                        //commitTime.Accumulate(result.CommitTimeInMs.TotalMilliseconds);
                        //getProposeTime.Accumulate(result.GetProposeCostTime.TotalMilliseconds);
                        //getProposeLockTime.Accumulate(result.GetProposeLockCostTime.TotalMilliseconds);
                        //prepareNewBallotTime.Accumulate(result.PrepareNewBallotCostTime.TotalMilliseconds);
                        //broadcaseQueryLastVoteTime.Accumulate(result.BroadcastQueryLastVoteCostTime.TotalMilliseconds);


                    });
                    var afterCreateTaskTime = DateTime.Now;
                    taskCreateTime.Accumulate((afterCreateTaskTime - beforeCreateTaskTime).TotalMilliseconds);

                    taskList.Add(task);
                    if (taskList.Count > 100)
                    {
                        await Task.WhenAll(taskList);
                        //var finishedTask = await Task.WhenAny(taskList);
                        //taskList.Remove(finishedTask);

                        /*DateTime firstFinishTime = DateTime.MaxValue;
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
                        */
                        taskList.Clear();
                        var end = DateTime.Now;
                        var costTime = (end - start).TotalMilliseconds;
                        var finishedCount = i /*- (UInt64)taskList.Count*/;
                        //if (finishedCount % 10 == 0)
                        {
                            Console.WriteLine("Request {0}, TPS: {1}", finishedCount, finishedCount * 1000 / costTime);
                            if (finishedCount % 2000 == 0)
                            {
                                LogStat("proposeDecreeTime", proposeDecreeTime);
                                LogStat("collectLastVoteTime", collectLastVoteTime);
                                LogStat("voteTime", voteTime);
                                LogStat("commitTime", commitTime);
                                LogStat("getProposeTime", getProposeTime);
                                LogStat("getProposeLockTime", getProposeLockTime);
                                LogStat("prepareNewBallotTime", prepareNewBallotTime);
                                LogStat("broadcaseQueryLastVoteTime", broadcaseQueryLastVoteTime);
                            }
                        }

                        beginWait = DateTime.Now;
                    }
                }

                await Task.WhenAll(taskList);

                foreach (var node in nodeMap)
                {
                    node.Value.Dispose();
                }
            }
        }
    }
}
