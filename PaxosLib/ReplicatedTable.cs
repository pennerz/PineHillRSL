using Paxos.Protocol;
using Paxos.Network;
using Paxos.Node;
using Paxos.Persistence;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Request;
using Paxos.Rpc;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Paxos.ReplicatedTable
{
    public class ReplicatedTableRequest
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }

    class RequestSerializer
    {
        public static string Serialize(ReplicatedTableRequest req)
        {
            return req.Key + "_" + req.Value;
        }

        public static ReplicatedTableRequest DeSerialize(string content)
        {
            var separatorIndex = content.IndexOf('_');
            return new ReplicatedTableRequest()
            {
                Key = content.Substring(0, separatorIndex),
                Value = content.Substring(separatorIndex + 1)
            };
        }
    }

    public class ReplicatedTable : StateMachine.PaxosStateMachine
    {
        Dictionary<string, string> _table = new Dictionary<string, string>();

        public ReplicatedTable(
            PaxosCluster cluster,
            NodeInfo nodeInfo) : base(cluster, nodeInfo)
        {
            
        }

        public async Task InstertTable(ReplicatedTableRequest tableRequest)
        {
            var request = new StateMachine.StateMachineRequest();
            request.Content = RequestSerializer.Serialize(tableRequest);
            await Request(request);
        }

        protected override Task ExecuteRequest(StateMachine.StateMachineRequest request)
        {
            var tableRequst = RequestSerializer.DeSerialize(request.Content);
            if (_table.ContainsKey(tableRequst.Key))
            {
                _table[tableRequst.Key] = tableRequst.Value;
            }
            else
            {
                _table.Add(tableRequst.Key, tableRequst.Value);
            }
            return Task.CompletedTask;
        }

    }
}
