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

        private ILogger _proposeLogger;
        private ILogger _voterLogger;

        private MetaNote _metaNote;
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

            var metaLogger = new FileLogger(".\\storage\\" + _nodeInfo.Name + ".meta");
            _proposeLogger = new FileLogger(".\\storage\\" + _nodeInfo.Name + ".proposerlog");
            _voterLogger = new FileLogger(".\\storage\\" + _nodeInfo.Name + ".voterlog");
            _voterNote = new VoterNote(_voterLogger);
            _proposerNote = new ProposerNote(_proposeLogger, metaLogger);
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

            _proposeLogger = new FileLogger(proposerLog);
            _voterLogger = new FileLogger(voterLog);

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
            await _proposerRole.Load();

            _messager = new PaxosNodeMessageDeliver(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);
            var taask = _rpcServer.Start();

        }
        public async Task Load(string metaLog)
        {
            Dispose();

            var metaLogger = new FileLogger(metaLog);

            //
            var fileBase = metaLog;
            var index = metaLog.IndexOf(".meta");
            if (index > 0)
            {
                fileBase = metaLog.Substring(0, index);
            }
            var proposerLog = fileBase + ".proposerlog";
            var voterLog = fileBase + ".voterlog";

            _proposeLogger = new FileLogger(proposerLog);
            _voterLogger = new FileLogger(voterLog);

            _voterNote = new VoterNote(_voterLogger);
            await _voterNote.Load();

            _proposerNote = new ProposerNote(_proposeLogger, metaLogger);
            await _proposerNote.Load();

            _proposeManager = new ProposeManager(_proposerNote.GetMaximumCommittedDecreeNo());

            _voterRole = new VoterRole(_nodeInfo, _cluster, _rpcClient, _voterNote, _proposerNote);
            _proposerRole = new ProposerRole(
                _nodeInfo, _cluster, _rpcClient, _proposerNote, _proposeManager);

            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);

            await _proposerRole.Load();

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
