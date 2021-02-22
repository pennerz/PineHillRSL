using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Paxos.Protocol;
using PineHillRSL.ReplicatedTable;
using PineHillRSL.Paxos.Persistence;
using PineHillRSL.Tests;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.Server
{
    class Program
    {
        static void LogStat(string name, Statistic stat)
        {
            Console.WriteLine("{0}: avg={1}, min={2}, max={3}", name, stat.Avg, stat.Min, stat.Max);
        }

        static async Task Test(string[] args)
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

            var cluster = new ConsensusCluster();
            for (int i = 0; i < 5; i++)
            {
                var node = new NodeInfo("127.0.0.1");
                var nodeAddr = new NodeAddress(node, 88 + i);
                cluster.Members.Add(nodeAddr);
            }

            var networkInfr = new TestNetworkInfr();
            NetworkFactory.SetNetworkCreator(new TestNetworkCreator(networkInfr, new NodeInfo("127.0.0.1")));

            if (false)
            {

                var tableNodeMap = new Dictionary<string, ReplicatedTable.ReplicatedTable>();
                foreach (var nodeAddr in cluster.Members)
                {
                    var node = new ReplicatedTable.ReplicatedTable(cluster, nodeAddr);
                    tableNodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(nodeAddr)] = node;
                }

                var start = DateTime.Now;

                List<Task> taskList = new List<Task>();
                var master = tableNodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(cluster.Members[0])];
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
                    await node.Value.DisposeAsync();
                }

            }
            else
            {
                var nodeMap = new Dictionary<string, PaxosNode>();
                foreach (var nodeAddr in cluster.Members)
                {
                    var node = new PaxosNode(cluster, nodeAddr);
                    nodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(nodeAddr)] = node;
                }

                bool isParallel = true;
                var start = DateTime.Now;

                var proposer = nodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(cluster.Members[0])];

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
                    var decree = new ConsensusDecree()
                    {
                        Data = randomData//data
                        //Content = "test" + i.ToString() + randomstr
                    };
                    var beforeCreateTaskTime = DateTime.Now;
                    var task = Task.Run(async () =>
                    {
                        var begin = DateTime.Now;
                        var result = await proposer.ProposeDecree(decree, 0/*nextDecreNo*/);
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
                    await node.Value.DisposeAsync();
                }
            }
        }


        private static void CleanupLogFiles(string nodeName)
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

        public static async Task StateMachinePefTest()
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

            LogSizeThreshold.CommitLogFileCheckpointThreshold = 10 * 1024 * 1024;

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
                tableNodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(nodeAddr)] = node;
            }
            Trace.WriteLine("All cluster nodes intialized");

            start = DateTime.Now;

            List<Task> taskList = new List<Task>();
            var master = tableNodeMap[PineHillRSL.Paxos.Node.PaxosNode.GetInstanceName(cluster.Members[0])];

            var totoalCount = 2400000;
            var finishedRequestCount = 0;
            for (int i = 0; i < totoalCount; i++)
            {
                var task = master.InstertTable(new ReplicatedTableRequest() { Key = i.ToString(), Value = "test" + i.ToString() });
                taskList.Add(task);
                if (taskList.Count > 100)
                {
                    //await Task.WhenAll(taskList);
                    while (taskList.Count > 0)
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
            Console.WriteLine($"TPS: {totoalCount / costTime * 1000 }");

            var avgTransmitTime = StatisticCounter.GetCounterAvg((int)PerCounter.NetworkPerfCounterType.NetworkMessageTransmitTime);
            var maxTransmitTime = StatisticCounter.GetMaximumValue((int)PerCounter.NetworkPerfCounterType.NetworkMessageTransmitTime);
            Console.WriteLine($"Transmit Time: avg: {avgTransmitTime}ms, maximum:{maxTransmitTime}ms");

            var avgRcvMsgDelayedTime = StatisticCounter.GetCounterAvg((int)PerCounter.NetworkPerfCounterType.NetworkMessageRecvDelayedTime);
            var maxRcvMsgDelayedTime = StatisticCounter.GetMaximumValue((int)PerCounter.NetworkPerfCounterType.NetworkMessageRecvDelayedTime);
            Console.WriteLine($"Received Message Delayed Time: avg: {avgRcvMsgDelayedTime}ms, maximum:{maxRcvMsgDelayedTime}ms");

            var avgProposeTime = StatisticCounter.GetCounterAvg((int)PerCounter.NetworkPerfCounterType.ProposeTime);
            var maxProposeTime = StatisticCounter.GetMaximumValue((int)PerCounter.NetworkPerfCounterType.ProposeTime);
            Console.WriteLine($"Propose Time: avg: {avgProposeTime}ms, maximum:{maxProposeTime}ms");

            var avgProposeTableLockWaitTime = StatisticCounter.GetCounterAvg((int)PerCounter.NetworkPerfCounterType.TableLockWaitTime);
            var maxProposeTableLockWaitTime = StatisticCounter.GetMaximumValue((int)PerCounter.NetworkPerfCounterType.TableLockWaitTime);
            Console.WriteLine($"Propose Table lock wait Time: avg: {avgProposeTableLockWaitTime}ms, maximum:{maxProposeTableLockWaitTime}ms");

            var checkpointData = StatisticCounter.GetCounterSum((int)PerCounter.NetworkPerfCounterType.CheckpointDataSize);
            var checkpointCount = StatisticCounter.GetCounterSum((int)PerCounter.NetworkPerfCounterType.CheckpointCount);
            Console.WriteLine($"Checkpoint count: {checkpointCount}, datasize:{checkpointData}");

            var avgCheckpointTableLockTime = StatisticCounter.GetCounterAvg((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime);
            var maxCheckpointTableLockTime = StatisticCounter.GetMaximumValue((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime);
            var sumCheckpointTableLockTime = StatisticCounter.GetCounterSum((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime);
            var checkpointTableLockCount = StatisticCounter.GetCounterCount((int)PerCounter.NetworkPerfCounterType.CheckpointTableLockTime);
            Console.WriteLine($"Checkpoint Table lock Time: avg: {avgCheckpointTableLockTime}ms, maximum:{maxCheckpointTableLockTime}ms, sum:{sumCheckpointTableLockTime}ms, count:{checkpointTableLockCount}");

            foreach (var node in tableNodeMap)
            {
                await node.Value.DisposeAsync();
            }
            Console.WriteLine($"Finish");

            LogSizeThreshold.ResetDefault();
        }

        static async Task Main(string[] args)
        {
            //await StateMachinePefTest();
            //await Test(args);
            
            var cfgFile = new FileStream("c:\\config\\config.json", FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            var dataBuf = new byte[cfgFile.Length];
            var readLen = await cfgFile.ReadAsync(dataBuf, 0, (int)(cfgFile.Length));
            var cfgStr = Encoding.UTF8.GetString(dataBuf, 0, readLen);
            var serverCfg = JsonSerializer.Deserialize<ServerConfig>(cfgStr);

            var cluster = new ConsensusCluster();
            foreach (var memberStr in serverCfg.RSLClusterServers)
            {
                var nodeAddr = NodeAddress.DeSerialize(memberStr);
                Logger.Log($"RSL Cluster Member:[{nodeAddr.Node.Name}:{nodeAddr.Port}");
                cluster.Members.Add(nodeAddr);

            }
            NetworkFactory.SetNetworkCreator(new TcpNetworkCreator());

            var serverAddr = NodeAddress.DeSerialize(serverCfg.RSLServerAddr);
            Logger.Log($"RSL Node:[{serverAddr.Node.Name}:{serverAddr.Port}");
            var serverNode = new ReplicatedTable.ReplicatedTable(cluster, serverAddr);

            var serviceServer = new PineHillRSL.ServerLib.PineHillRSLServer(serverNode);
            var serviceAddr = NodeAddress.DeSerialize(serverCfg.ServiceServerAddr);
            Logger.Log($"Service Node:[{serviceAddr.Node.Name}:{serviceAddr.Port}");
            await serviceServer.StartServer(serviceAddr);
            Logger.Log($"Service Start Succeed!");
            while (true)
            {
                await Task.Delay(1000);
            }
        }
    }
}
