using Paxos.Protocol;
using Paxos.Network;
using Paxos.Persistence;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.MessageDelivery;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Node
{
    public class PaxosNode
    {
        private readonly DecreeLockManager _decreeLockManager;
        private readonly VoterRole _voterRole;
        private readonly ProposerRole _proposerRole;

        private readonly PaxosCluster _cluster;
        private readonly NodeInfo _nodeInfo;

        private readonly PaxosNodeMessageDelivery _messager;

        //private readonly Task _messageHandlerTask;

        private NetworkServer _networkSever;


        public PaxosNode(
            NetworkServer networkServer,
            PaxosCluster cluster,
            NodeInfo nodeInfo)
        {
            /*
            if (messageTransport == null)
            {
                throw new ArgumentNullException("MessageTransport is null");
            }*/

            if (cluster == null)
            {
                throw new ArgumentNullException("cluster is null");
            }
            if (nodeInfo == null)
            {
                throw new ArgumentNullException("nodeInfo is null");
            }

            //_messageTransport = messageTransport;
            _cluster = cluster;
            _nodeInfo = nodeInfo;

            _decreeLockManager = new DecreeLockManager();

            var persistenter = new MemoryPaxosNotePersistent();

            var ledger = new Ledger();
            var voterNote = new VoterNote(persistenter);
            _voterRole = new VoterRole(_nodeInfo, _cluster, networkServer, _decreeLockManager, voterNote, ledger);
            var proposerNote = new ProposerNote(ledger);
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, networkServer, _decreeLockManager, proposerNote, ledger);

            _networkSever = networkServer;
            _messager = new PaxosNodeMessageDelivery(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _networkSever.RegisterRequestHandler(rpcRequestHandler);
        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //

            return _proposerRole.BeginNewPropose(decree, decreeNo);
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            var result = await _proposerRole.ReadDecree(decreeNo);
            return result;
        }
    }
}
