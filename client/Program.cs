using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Paxos.Protocol;
using PineHillRSL.Paxos.Request;
using PineHillRSL.ReplicatedTable;
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

namespace PineHillRSL.Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cfgFile = new FileStream(".\\config.json", FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            var dataBuf = new byte[cfgFile.Length];
            var readLen = await cfgFile.ReadAsync(dataBuf, 0, (int)(cfgFile.Length));
            var cfgStr = Encoding.UTF8.GetString(dataBuf, 0, readLen);
            var clientCfg = JsonSerializer.Deserialize<ClientConfig>(cfgStr);

            NetworkFactory.SetNetworkCreator(new TcpNetworkCreator());

            var client = new ClientLib.PineHillRSLClient(clientCfg.ServiceServerAddrs);
            await client.InsertTable("1", "test1");
            await client.InsertTable("2", "test2");
            await client.InsertTable("3", "test3");

            var result = await client.ReadTable("1");
            result = await client.ReadTable("2");
            result = await client.ReadTable("3");

        }
    }
}
