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
        public byte[] RequestContent { get; set; }
        public bool NeedResp { get; set; }

        public byte[] Serialize()
        {
            var requestIdData = RequestId.ToByteArray();
            var isRequestData = BitConverter.GetBytes(IsRequest);
            var needRespData = BitConverter.GetBytes(NeedResp);
            var requestContentData = RequestContent;
            var dataList = new List<byte[]>();
            dataList.Add(requestIdData);
            dataList.Add(isRequestData);
            dataList.Add(needRespData);
            if (requestContentData != null)
                dataList.Add(requestContentData);

            var serializeBuf = new SerializeBuffer();
            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
            //return "RequestId:" + RequestId.ToString() + ";" +
            //"IsRequest:" + IsRequest.ToString() + ";" +
            //"NeedResp:" + NeedResp.ToString() + ";" +
            //"RequestContent:" + RequestContent;
        }

        public void DeSerialize(byte[] data)
        {
            var serializeBuf = new SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var endIt = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(endIt))
            {
                return;
            }
            var requestIdData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, requestIdData, 0, it.RecordSize);
            RequestId = new Guid(requestIdData);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            IsRequest = BitConverter.ToBoolean(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            NeedResp = BitConverter.ToBoolean(it.DataBuff, it.RecordOff);

            it = it.Next();
            if (it.Equals(endIt))
            {
                return;
            }
            RequestContent = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, RequestContent, 0, it.RecordSize);
            //RequestContent = Encoding.ASCII.GetString(it.DataBuff, it.RecordOff, it.RecordSize);

            //{
            //    var index = str.IndexOf(';');
            //    string subStr;
            //    if (index != -1)
            //    {
            //        subStr = str.Substring(0, index);
            //        str = str.Substring(index + 1);
            //    }
            //    else
            //    {
            //        subStr = str;
            //        str = null;
            //    }
            //    index = subStr.IndexOf(':');
            //    if (index == -1) continue;
            //    var name = subStr.Substring(0, index);
            //    var value = subStr.Substring(index + 1);
            //    if (name.Equals("RequestId"))
            //   {
            //        Guid result;
            //        Guid.TryParse(value, out result);
            //        RequestId = result;
            //    }
            //    else if (name.Equals("IsRequest"))
            //    {
            //        bool result = false;
            //        bool.TryParse(value, out result);
            //        IsRequest = result;
            //    }
            //    else if (name.Equals("NeedResp"))
            //    {
            //        bool result = false;
            //        bool.TryParse(value, out result);
            //        NeedResp = result;
            //    }
            //    else if (name.Equals("RequestContent"))
            //    {
            //        RequestContent = value;
            //        break;
            //    }
            //}
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
