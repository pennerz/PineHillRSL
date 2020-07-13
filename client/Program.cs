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
        static async Task Main(string[] args)
        {
            var client = new ClientLib.PineRSLClient();
            await client.InsertTable("1", "test1");
            await client.InsertTable("2", "test2");
            await client.InsertTable("3", "test3");
        }
    }
}
