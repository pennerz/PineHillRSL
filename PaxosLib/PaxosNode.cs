using PineRSL.Network;
using PineRSL.Paxos.Message;
using PineRSL.Paxos.Notebook;
using PineRSL.Paxos.Persistence;
using PineRSL.Paxos.Protocol;
using PineRSL.Paxos.Request;
using PineRSL.Paxos.Rpc;
using PineRSL.Rpc;
using System;
using System.Threading.Tasks;

namespace PineRSL.Paxos.Node
{
    public class PaxosNode : IDisposable
    {
        private bool _inLoading = false;
        private VoterRole _voterRole;
        private ProposerRole _proposerRole;

        private readonly PaxosCluster _cluster;
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

        private IPaxosNotification _notificationSubscriber;

        public enum DataSource { Local, Cluster};

        public PaxosNode(
            PaxosCluster cluster,
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

        public void Dispose()
        {
            Cleanup();
        }

        public void SubscribeNotification(IPaxosNotification listener)
        {
            _notificationSubscriber = listener;
            _proposerRole.SubscribeNotification(_notificationSubscriber);
            _voterRole.SubscribeNotification(_notificationSubscriber);
        }

        public async Task Load(string metaLog, ProposerRole.DataSource datasource = ProposerRole.DataSource.Local)
        {
            await Task.Run(() =>
            {
                _metaLogger?.Dispose();
            });
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

            await Task.Run(() =>
            {
                _proposeLogger?.Dispose();
            });
            _proposeLogger = new FileLogger(proposerLog);
            await Task.Run(() =>
            {
                _voterLogger?.Dispose();
            });
            _voterLogger = new FileLogger(voterLog);

            _voterNote = new VoterNote(_voterLogger);
            await _voterNote.Load();

            _proposerNote = new ProposerNote(_proposeLogger, _metaLogger);
            await _proposerNote.Load();

            _proposeManager = new ProposeManager(_proposerNote.GetMaximumCommittedDecreeNo());

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

        public async Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
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

        private void Init()
        {
            //var localAddr = new NodeAddress(_nodeInfo, 0);
            _rpcClient = new RpcClient(/*localAddr*/);
            //var serverAddr = new NodeAddress(_nodeInfo, 88);
            Common.Logger.Log($"RSL Node Address: {_serverAddr.Node.Name}:{_serverAddr.Port}");
            _rpcServer = new RpcServer(_serverAddr);

            _rpcServer.Start().Wait();
            Common.Logger.Log($"RSL Node RPC Server Startd");

            var instanceName = NodeAddress.Serialize(_serverAddr);

            var metaLogFilePath = ".\\storage\\" + instanceName + ".meta";
            var task = Load(metaLogFilePath);
            task.Wait();
            Common.Logger.Log($"RSL Node Loaded");
        }

        private void Cleanup()
        {
            _rpcClient?.Dispose();
            _rpcClient = null;
            _rpcServer?.Dispose();
            _rpcServer = null;

            _voterRole?.Dispose();
            _voterRole = null;

            _proposerRole?.Dispose();
            _proposerRole = null;

            _metaLogger?.Dispose();
            _metaLogger = null;
            _proposeLogger?.Dispose();
            _proposeLogger = null;
            _voterLogger?.Dispose();
            _voterLogger = null;

            _voterNote?.Dispose();
            _voterNote = null;
            _proposerNote?.Dispose();
            _proposerNote = null;

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
