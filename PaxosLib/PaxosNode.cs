using Paxos.Protocol;
using Paxos.Network;
using Paxos.Persistence;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.Request;
using Paxos.Rpc;
using System;
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

        private readonly PaxosNodeMessageDeliver _messager;

        //private readonly Task _messageHandlerTask;

        //private NetworkServer _networkSever;
        private RpcClient _rpcClient;
        private RpcServer _rpcServer;


        public PaxosNode(
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

            var localAddr = new NodeAddress()
            {
                Node = _nodeInfo,
                Port = 0
            };
            _rpcClient = new RpcClient(localAddr);
            var serverAddr = new NodeAddress()
            {
                Node = _nodeInfo,
                Port = 88
            };
            _rpcServer = new RpcServer(serverAddr);

            _decreeLockManager = new DecreeLockManager();
            var ledger = new Ledger(".\\loegger" + _nodeInfo.Name + ".log");
            var votedLogger = new FilePaxosVotedBallotLog(".\\votedlogger_" + _nodeInfo.Name + ".log");

            var voterNote = new VoterNote(votedLogger);
            _voterRole = new VoterRole(_nodeInfo, _cluster, _rpcClient, _decreeLockManager, voterNote, ledger);
            var proposerNote = new ProposerNote(ledger);
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, _rpcClient, _decreeLockManager, proposerNote, ledger);

            _messager = new PaxosNodeMessageDeliver(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);
            var taask = _rpcServer.Start();

            //_networkSever = networkServer;
            //_networkSever.RegisterRequestHandler(rpcRequestHandler);
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
