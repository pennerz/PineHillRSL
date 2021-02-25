using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Network;
using PineHillRSL.Paxos.Message;
using PineHillRSL.Paxos.Notebook;
using PineHillRSL.Paxos.Protocol;
using PineHillRSL.Paxos.Rpc;
using PineHillRSL.Rpc;
using System;
using System.Threading.Tasks;

namespace PineHillRSL.Paxos.Node
{
    public class PaxosNode : IConsensusNode
    {
        private bool _inLoading = false;
        private VoterRole _voterRole;
        private ProposerRole _proposerRole;

        private readonly ConsensusCluster _cluster;
        private readonly NodeAddress _serverAddr;

        private PaxosNodeMessageDeliver _messager;

        private RpcClient _rpcClient;
        private RpcServer _rpcServer;

        private ILogger _metaLogger;
        private ILogger _proposeLogger;
        private ILogger _voterLogger;

        private VoterNote _voterNote;
        private ProposerNote _proposerNote;
        private ProposeManager _proposeManager;

        private IConsensusNotification _notificationSubscriber;

        //public enum DataSource { Local, Cluster};

        public PaxosNode(
            ConsensusCluster cluster,
            NodeAddress serverAddr)
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
            if (serverAddr == null)
            {
                throw new ArgumentNullException("nodeInfo is null");
            }

            //_messageTransport = messageTransport;
            _cluster = cluster;
            _serverAddr = serverAddr;

            Init();

        }

        public async ValueTask DisposeAsync()
        {
            await Cleanup();
        }

        public void SubscribeNotification(IConsensusNotification listener)
        {
            _notificationSubscriber = listener;
            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);
        }

        public async Task Load(string metaLog, DataSource datasource = DataSource.Local)
        {
            if (_metaLogger != null)
                await _metaLogger.DisposeAsync();
            _metaLogger = new FileLogger(metaLog);

            //
            var fileBase = metaLog;
            var index = metaLog.IndexOf(".meta");
            if (index > 0)
            {
                fileBase = metaLog.Substring(0, index);
            }
            var proposerLog = fileBase + ".proposerlog";
            var voterLog = fileBase + ".voterlog";

            if (_proposeLogger != null)
                await _proposeLogger.DisposeAsync();
            _proposeLogger = new FileLogger(proposerLog);

            if (_voterLogger != null)
                await _voterLogger.DisposeAsync();
            _voterLogger = new FileLogger(voterLog);

            _voterNote = new VoterNote(_voterLogger);
            await _voterNote.Load();

            _proposerNote = new ProposerNote(_proposeLogger, _metaLogger);
            await _proposerNote.Load();

            _proposeManager = new ProposeManager(await _proposerNote.GetMaximumCommittedDecreeNo());

            _voterRole = new VoterRole(_serverAddr, _cluster, _rpcClient, _voterNote, _proposerNote);
            _proposerRole = new ProposerRole(
                _serverAddr, _cluster, _rpcClient, _proposerNote, _proposeManager);

            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);

            _messager = new PaxosNodeMessageDeliver(_proposerRole, _voterRole);
            var rpcRequestHandler = new PaxosMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);

            await _proposerRole.Load(datasource);


            // request checkpoint from remote nodes

        }

        public async Task<ProposeResult> ProposeDecree(ConsensusDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //
            using (var activity = Common.ActivityControl.NewActivity())
            {
                ProposeResult result = null;
                // propose
                try
                {
                    result = await _proposerRole.Propose(decree, decreeNo);
                }
                catch (Exception e)
                {
                    // need to load exceptions from remote node
                    throw e;
                }
                return result;
            }
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            using (var activity = Common.ActivityControl.NewActivity())
            {
                var result = await _proposerRole.ReadDecree(decreeNo);
                return result;
            }
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

        public ulong MaxCommittedNo => _proposerNote.GetMaximumCommittedDecreeNo().Result;

        private void Init()
        {
            //var localAddr = new NodeAddress(_nodeInfo, 0);
            _rpcClient = new RpcClient(/*localAddr*/);
            //var serverAddr = new NodeAddress(_nodeInfo, 88);
            Common.Logger.Log($"RSL Node Address: {_serverAddr.Node.Name}:{_serverAddr.Port}");
            _rpcServer = new RpcServer(_serverAddr);

            _rpcServer.Start().Wait();
            Common.Logger.Log($"RSL Node RPC Server Startd");

            var instanceName = ConsensusNodeHelper.GetInstanceName(_serverAddr);

            var metaLogFilePath = ".\\storage\\" + instanceName + ".meta";
            var task = Load(metaLogFilePath);
            task.Wait();
            Common.Logger.Log($"RSL Node Loaded");
        }

        private async Task Cleanup()
        {
            if (_rpcClient != null)
                await _rpcClient.DisposeAsync();
            _rpcClient = null;
            if (_rpcServer != null)
                await _rpcServer.DisposeAsync();
            _rpcServer = null;

            if (_voterRole != null)
                await _voterRole.DisposeAsync();
            _voterRole = null;

            if (_proposerRole != null)
                await _proposerRole.DisposeAsync();
            _proposerRole = null;

            if (_voterNote != null)
                await _voterNote.DisposeAsync();
            _voterNote = null;

            if (_proposerNote != null)
                await _proposerNote.DisposeAsync();
            _proposerNote = null;

            if (_metaLogger != null)
                await _metaLogger.DisposeAsync();
            _metaLogger = null;

            if (_proposeLogger != null)
                await _proposeLogger.DisposeAsync();
            _proposeLogger = null;

            if (_voterLogger != null)
                await _voterLogger.DisposeAsync();
            _voterLogger = null;

            _proposeManager?.Dispose();
            _proposeManager = null;

            _messager?.Dispose();
            _messager = null;
        }


        private async Task LoadCheckpointFromRemoteNode()
        {
            _inLoading = true;

            // clear current instances

            // build new

            //
            await _proposerRole.Load();

            _inLoading = false;
        }
    }
}
