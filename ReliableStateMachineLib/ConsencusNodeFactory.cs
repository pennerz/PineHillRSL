using PineHillRSL.Consensus.Node;
using PineHillRSL.Paxos.Node;
using PineHillRSL.Common;
using PineHillRSL.Network;
using PineHillRSL.Raft.Node;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.StateMachine
{
    public class ConsencusNodeFactory
    {
        public enum ConcensusProtocol { Paxos, Raft};

        static ConcensusProtocol s_protocol = ConcensusProtocol.Paxos;
        public static void SetConsensusProtocol(ConcensusProtocol protocol)
        {
            s_protocol = protocol;
        }

        public static IConsensusNode CreateConsensusNode(
            ConsensusCluster cluster,
            NodeAddress serverAddr)
        {
            switch (s_protocol)
            {
                case ConcensusProtocol.Raft:
                    return new RaftNode(cluster, serverAddr);
                case ConcensusProtocol.Paxos:
                default:
                    return new PaxosNode(cluster, serverAddr);
            }
        }
    }
}
