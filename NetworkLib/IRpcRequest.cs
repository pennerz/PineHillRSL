using Paxos.Network;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using System.Text.Json;
using System.Runtime.Serialization.Json;
using System.Text.Json.Serialization;
using Paxos.Common;

namespace Paxos.Rpc
{
    /// <summary>
    /// Rpc message communicated between client and server
    /// </summary>
    [Serializable()]
    public class RpcMessage : ISer
    {
        public RpcMessage()
        {
            RequestId = Guid.NewGuid();
            IsRequest = true;
            NeedResp = true;
        }

        public Guid RequestId { get; set; }
        public bool IsRequest { get; set; }
        public string RequestContent { get; set; }
        public bool NeedResp { get; set; }

        public string Serialize()
        {
            return "RequestId:" + RequestId.ToString() + ";" +
            "IsRequest:" + IsRequest.ToString() + ";" +
            "RequestContent:" + RequestContent + ";" +
            "NeedResp:" + NeedResp.ToString() + ";";
        }

        public void DeSerialize(string str)
        {
            while (str != null)
            {
                var index = str.IndexOf(';');
                string subStr;
                if (index != -1)
                {
                    subStr = str.Substring(0, index);
                    str = str.Substring(index + 1);
                }
                else
                {
                    subStr = str;
                    str = null;
                }
                index = subStr.IndexOf(':');
                if (index == -1) continue;
                var name = subStr.Substring(0, index);
                var value = subStr.Substring(index + 1);
                if (name.Equals("RequestId"))
                {
                    Guid result;
                    Guid.TryParse(value, out result);
                    RequestId = result;
                }
                else if (name.Equals("IsRequest"))
                {
                    bool result = false;
                    bool.TryParse(value, out result);
                    IsRequest = result;
                }
                else if (name.Equals("RequestContent"))
                {
                    RequestContent = value;
                }
                else if (name.Equals("NeedResp"))
                {
                    bool result = false;
                    bool.TryParse(value, out result);
                    NeedResp = result;
                }
            }
        }
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
