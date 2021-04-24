using PineHillRSL.Consensus.Node;
using PineHillRSL.Consensus.Persistence;
using PineHillRSL.Consensus.Request;
using PineHillRSL.Network;
using PineHillRSL.Raft.Message;
using PineHillRSL.Raft.Notebook;
using PineHillRSL.Raft.Protocol;
using PineHillRSL.Raft.Rpc;
using PineHillRSL.Rpc;
using System;
using System.Threading.Tasks;

namespace PineHillRSL.Raft.Node
{
    public class RaftNode : IConsensusNode
    {
        private bool _inLoading = false;

        private readonly ConsensusCluster _cluster;
        private readonly NodeAddress _serverAddr;

        private RaftNodeMessageDeliver _messager;

        private RpcClient _rpcClient;
        private RpcServer _rpcServer;


        private RaftRole _role;

        private ILogger _metaLogger = null;
        private ILogger _entityLogger = null;
        private EntityNote _entityNote = null;


        private IConsensusNotification _notificationSubscriber;


        public RaftNode(
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
            var entityLog = fileBase + ".entitylog";

            if (_entityLogger != null)
                await _entityLogger.DisposeAsync();
            _entityLogger = new FileLogger(entityLog);
            _entityNote = new EntityNote(_entityLogger, _metaLogger);

            _role = new RaftRole(_serverAddr, _rpcClient, _cluster, _entityNote);

            _messager = new RaftNodeMessageDeliver(_role);
            var rpcRequestHandler = new RaftMessageHandler(_messager, null);
            _rpcServer.RegisterRequestHandler(rpcRequestHandler);


            await _role.Load(DataSource.Local);

            // request checkpoint from remote nodes

        }

        public async Task<ProposeResult> ProposeDecree(ConsensusDecree decree, ulong decreeNo)
        {
            //
            // 1. get leader node. If not leader node, return leader node to cleint
            // 2. if leader, broadcase decree to all followers
            // 3. commit decree when received success response from majority followers
            //

            /*
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
            }*/
            return await _role.Propose(decree, decreeNo);
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            /*
            using (var activity = Common.ActivityControl.NewActivity())
            {
                var result = await _proposerRole.ReadDecree(decreeNo);
                return result;
            }*/
            return null;
        }

        public bool NotifyLearner
        {
            get
            {
                //return _proposerRole.NotifyLearners;
                return true;
            }
            set
            {
                //_proposerRole.NotifyLearners = value;
                
            }
        }

        public ulong MaxCommittedNo => 0; // _proposerNote.GetMaximumCommittedDecreeNo().Result;

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


            if (_entityNote != null)
                await _entityNote.DisposeAsync();
            _entityNote = null;

            if (_metaLogger != null)
                await _metaLogger.DisposeAsync();
            _metaLogger = null;


            if (_entityLogger != null)
                await _entityLogger.DisposeAsync();
            _entityLogger = null;

            _messager?.Dispose();
            _messager = null;
        }


        private async Task LoadCheckpointFromRemoteNode()
        {
            _inLoading = true;

            // clear current instances

            // build new

            //
            _inLoading = false;
        }
    }
}
