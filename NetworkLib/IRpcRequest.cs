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
    /// <summary>
    /// Rpc message communicated between client and server
    /// </summary>
    [Serializable()]
    public class RpcMessage
    {
        public RpcMessage()
        {
            RequestId = Guid.NewGuid();
            IsRequest = true;
        }

        public Guid RequestId { get; set; }
        public bool IsRequest { get; set; }
        public string RequestContent { get; set; }
    }

    /// <summary>
    /// Rpc request handler
    /// </summary>
    public interface IRpcRequestHandler
    {
        Task<RpcMessage> HandleRequest(RpcMessage request);
    }

    /// <summary>
    /// Rpc client interface
    /// </summary>
    public interface IRpcClient
    {
        Task<RpcMessage> SendRequest(NodeAddress targetNode, RpcMessage request);
    }

    /// <summary>
    /// Rpc server interface
    /// </summary>
    public interface IRpcServer
    {
        void RegisterRequestHandler(IRpcRequestHandler requestHandler);
    }
}
