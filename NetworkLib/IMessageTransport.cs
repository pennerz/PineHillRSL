using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Paxos.Network
{
    public class NodeInfo
    {
        public string Name { get; set; }

    }

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

    public interface INetworkTransport
    {
        Task SendMessage(RpcMessage msg);

        Task<RpcMessage> ReceiveMessage();
    }
}
