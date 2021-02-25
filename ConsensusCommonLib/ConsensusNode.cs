using PineHillRSL.Consensus.Request;
using PineHillRSL.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace PineHillRSL.Consensus.Node
{
    public class ConsensusCluster
    {
        private List<NodeAddress> _members = new List<NodeAddress>();

        public List<NodeAddress> Members => _members;
    }


    public interface IConsensusNotification
    {
        Task UpdateSuccessfullDecree(UInt64 decreeNo, ConsensusDecree decree);
        Task<UInt64> Checkpoint(Stream checkpointStream);
        Task LoadCheckpoint(UInt64 decreeNo, Stream checkpointStream);
    }

    public enum DataSource { Local, Cluster };

    public interface IConsensusNode : IAsyncDisposable
    {
        void SubscribeNotification(IConsensusNotification listener);

        Task Load(string metaLog, DataSource datasource = DataSource.Local);

        Task<ProposeResult> ProposeDecree(ConsensusDecree decree, ulong decreeNo);

        Task<DecreeReadResult> ReadDecree(ulong decreeNo);

        bool NotifyLearner { get; set; }

        ulong MaxCommittedNo { get; }

    }

    public class ConsensusNodeHelper
    {
        public static string GetInstanceName(NodeAddress nodeAddr)
        {
            var instanceName = NodeAddress.Serialize(nodeAddr);
            instanceName = instanceName.Replace(":", "_");
            return instanceName;
        }
    }
}
