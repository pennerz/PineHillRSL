using PineHillRSL.Consensus.Node;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Common;
using PineHillRSL.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.StateMachine
{
    public class ConsencusNodeFactory
    {
        public static IConsensusNode CreateConsensusNode(
            ConsensusCluster cluster,
            NodeAddress serverAddr)
        {
            return new PaxosNode(cluster, serverAddr);
        }
    }
}
