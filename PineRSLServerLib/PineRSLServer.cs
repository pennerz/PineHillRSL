using PineRSL.ClientLib;
using PineRSL.Rpc;
using PineRSL.Network;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PineRSL.ServerLib
{

    public class PineRSLServerRequestHandler : IRpcRequestHandler
    {
        private ReplicatedTable.ReplicatedTable _rtable;
        public PineRSLServerRequestHandler(ReplicatedTable.ReplicatedTable rTableNode)
        {
            _rtable = rTableNode;
        }

        public async Task<RpcMessage> HandleRequest(RpcMessage requestMsg)
        {
            var rpcRequest = PineRequetMessageFactory.CreateRpcRequest(requestMsg);
            if (rpcRequest == null)
            {
                return null;
            }
            switch(rpcRequest.Type)
            {
                case PineRSLRpcRequest.RequestType.InsertTable:
                    if (_rtable == null)
                    {
                        return null;
                    }
                    var serverRequest = new ReplicatedTable.ReplicatedTableRequest()
                    {
                        Key = ((InsertTableRequest)rpcRequest).Key,
                        Value = ((InsertTableRequest)rpcRequest).Value
                    };
                    await _rtable.InstertTable(serverRequest);
                    return null;
                case PineRSLRpcRequest.RequestType.Unknown:
                default:
                    break;
            }
            return null;
        }
    }

    public class PineRSLServer
    {
        private RpcServer _rpcserver;
        private PineRSLServerRequestHandler _handler;

        public PineRSLServer(ReplicatedTable.ReplicatedTable rTableNode)
        {
            _handler = new PineRSLServerRequestHandler(rTableNode);
        }

        public async Task StartServer(NodeAddress serverAddr)
        {
            _rpcserver = new RpcServer(serverAddr);
            _rpcserver.RegisterRequestHandler(_handler);
            await _rpcserver.Start();
        }

    }
}
