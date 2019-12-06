using Paxos.Network;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Paxos.Rpc
{
 
    public interface IRpcRequestHandler
    {
        Task<RpcMessage> HandleRequest(RpcMessage request);
    }

    public interface IRpcTransport
    {
        Task<RpcMessage> SendRequest(RpcMessage request);
        void RegisterRequestHandler(IRpcRequestHandler requestHandler);
    }

    public interface IRpcClient
    {
        Task<RpcMessage> SendRequest(NodeAddress targetNode, RpcMessage request);
    }

    public interface IRpcServer
    {
        void RegisterRequestHandler(IRpcRequestHandler requestHandler);
    }
}
