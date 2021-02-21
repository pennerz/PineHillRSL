using PineHillRSL.ClientLib;
using PineHillRSL.Rpc;
using PineHillRSL.Network;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PineHillRSL.ServerLib
{

    public class PineHillRSLServerRequestHandler : IRpcRequestHandler
    {
        private ReplicatedTable.ReplicatedTable _rtable;
        public PineHillRSLServerRequestHandler(ReplicatedTable.ReplicatedTable rTableNode)
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
                case PineHillRSLRpcRequest.RequestType.InsertTable:
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
                    PineHillRSL.Common.Logger.Log($"Insert table row[key:{serverRequest.Key}, value:{serverRequest.Value}");
                    return null;
                case PineHillRSLRpcRequest.RequestType.ReadTable:
                    if (_rtable == null)
                    {
                        return null;
                    }
                    var value = await _rtable.ReadTable(((ReadTableRequest)rpcRequest).Key);
                    ((ReadTableRequest)rpcRequest).Value = value;
                    return PineRequetMessageFactory.CreateRpcMessage((ReadTableRequest)rpcRequest);
                case PineHillRSLRpcRequest.RequestType.Unknown:
                default:
                    break;
            }
            return null;
        }
    }

    public class PineHillRSLServer
    {
        private RpcServer _rpcserver;
        private PineHillRSLServerRequestHandler _handler;

        public PineHillRSLServer(ReplicatedTable.ReplicatedTable rTableNode)
        {
            _handler = new PineHillRSLServerRequestHandler(rTableNode);
        }

        public async Task StartServer(NodeAddress serverAddr)
        {
            _rpcserver = new RpcServer(serverAddr);
            _rpcserver.RegisterRequestHandler(_handler);
            await _rpcserver.Start();
        }

    }
}
