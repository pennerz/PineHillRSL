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
    public class PaxosNode : IDisposable
    {
        private VoterRole _voterRole;
        private ProposerRole _proposerRole;

        private readonly PaxosCluster _cluster;
        private readonly NodeInfo _nodeInfo;

        private PaxosNodeMessageDeliver _messager;

        private RpcClient _rpcClient;
        private RpcServer _rpcServer;

        private FilePaxosCommitedDecreeLog _proposeLogger;
        private FilePaxosVotedBallotLog _voterLogger;

        private VoterNote _voterNote;
        private ProposerNote _proposerNote;
        private ProposeManager _proposeManager;

        private IPaxosNotification _notificationSubscriber;


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

            var localAddr = new NodeAddress(_nodeInfo, 0);
            _rpcClient = new RpcClient(localAddr);
            var serverAddr = new NodeAddress(_nodeInfo, 88);
            _rpcServer = new RpcServer(serverAddr);


            _proposeLogger = new FilePaxosCommitedDecreeLog(".\\loegger" + _nodeInfo.Name + ".log");
            _voterLogger = new FilePaxosVotedBallotLog(".\\votedlogger_" + _nodeInfo.Name + ".log");
            _voterNote = new VoterNote(_voterLogger);
            _proposerNote = new ProposerNote(_proposeLogger);
            _proposeManager = new ProposeManager(_proposerNote.GetMaximumCommittedDecreeNo());

            _voterRole = new VoterRole(_nodeInfo, _cluster, _rpcClient, _voterNote, _proposerNote);
            _proposerRole = new ProposerRole(
                _nodeInfo, _cluster, _rpcClient, _proposerNote, _proposeManager);

            _messager = new PaxosNodeMessageDeliver(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);
            var taask = _rpcServer.Start();
        }

        public void Dispose()
        {
            _messager?.Dispose();
            _voterRole?.Dispose();
            _proposerRole?.Dispose();
            _proposeManager?.Dispose();
            _voterNote?.Dispose();
            _proposerNote?.Dispose();
            _proposeLogger?.Dispose();
            _voterLogger?.Dispose();
        }
        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);
        }
        public async Task Load(string proposerLog, string voterLog)
        {
            Dispose();

            _proposeLogger = new FilePaxosCommitedDecreeLog(proposerLog);
            _voterLogger = new FilePaxosVotedBallotLog(voterLog);

            _voterNote = new VoterNote(_voterLogger);
            await _voterNote.Load();

            _proposerNote = new ProposerNote(_proposeLogger);
            await _proposerNote.Load();

            _proposeManager = new ProposeManager(_proposerNote.GetMaximumCommittedDecreeNo());

            _voterRole = new VoterRole(_nodeInfo, _cluster, _rpcClient, _voterNote, _proposerNote);
            _proposerRole = new ProposerRole(
                _nodeInfo, _cluster, _rpcClient, _proposerNote, _proposeManager);

            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);


            _messager = new PaxosNodeMessageDeliver(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);
            var taask = _rpcServer.Start();

        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //

            return _proposerRole.Propose(decree, decreeNo);
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            var result = await _proposerRole.ReadDecree(decreeNo);
            return result;
        }

        public bool NotifyLearner
        {
            get
            {
                return _proposerRole.NotifyLearners;
            }
            set
            {
                _proposerRole.NotifyLearners = value;
            }
        }
    }
}
