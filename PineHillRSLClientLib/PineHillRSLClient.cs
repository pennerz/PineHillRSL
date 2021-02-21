using PineHillRSL.Rpc;
using PineHillRSL.Network;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace PineHillRSL.ClientLib
{
    public class PineHillRSLRpcRequest : Common.ISer
    {
        public enum RequestType {Unknown, InsertTable, ReadTable};

        public PineHillRSLRpcRequest()
        {
            Type = RequestType.Unknown;
            TimeoutInMs = 30 * 1000;
        }

        public RequestType Type { get; set; }
        public UInt64 TimeoutInMs { get; set; }

        public virtual byte[] Serialize()
        {
            return null;
        }

        public virtual void DeSerialize(byte[] data)
        {

        }
    }

    public class InsertTableRequest :  PineHillRSLRpcRequest
    {
        private string _key;
        private string _value;

        public InsertTableRequest(string key, string value)
        {
            Type = RequestType.InsertTable;
            _key = key;
            _value = value;
        }

        public InsertTableRequest()
        {
            Type = RequestType.InsertTable;
        }

        public string Key => _key;
        public string Value => _value;

        public override byte[] Serialize()
        {
            var serializeBuf = new Common.SerializeBuffer();
            var messageTypeData = BitConverter.GetBytes((int)Type);
            var keyData = Encoding.UTF8.GetBytes(Key);
            var valueData = Encoding.UTF8.GetBytes(Value);
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(keyData);
            dataList.Add(valueData);
            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuf = new Common.SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var itEnd = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            var requestType = (PineHillRSLRpcRequest.RequestType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            var keyData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, keyData, 0, it.RecordSize);
            _key = Encoding.UTF8.GetString(keyData);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            var valueData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, valueData, 0, it.RecordSize);
            _value = Encoding.UTF8.GetString(valueData);
        }
    }

    public class ReadTableRequest : PineHillRSLRpcRequest
    {
        private string _key;

        public ReadTableRequest(string key)
        {
            Type = RequestType.ReadTable;
            _key = key;
        }

        public ReadTableRequest()
        {
            Type = RequestType.ReadTable;
        }

        public string Key => _key;
        public string Value { get; set; } = "";

        public override byte[] Serialize()
        {
            var serializeBuf = new Common.SerializeBuffer();
            var messageTypeData = BitConverter.GetBytes((int)Type);
            var keyData = Encoding.UTF8.GetBytes(Key);
            var valueData = Encoding.UTF8.GetBytes(Value);
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(keyData);
            dataList.Add(valueData);
            serializeBuf.AppendBlocks(dataList);
            return serializeBuf.DataBuf;
        }

        public override void DeSerialize(byte[] data)
        {
            var serializeBuf = new Common.SerializeBuffer();
            serializeBuf.ConcatenateBuff(data);
            var itEnd = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(itEnd))
            {
                return;
            }
            var requestType = (PineHillRSLRpcRequest.RequestType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            var keyData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, keyData, 0, it.RecordSize);
            _key = Encoding.UTF8.GetString(keyData);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return;
            }
            var valueData = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, valueData, 0, it.RecordSize);
            Value = Encoding.UTF8.GetString(valueData);
        }
    }



    public class PineRequetMessageFactory
    {
        public static PineHillRSLRpcRequest CreateRpcRequest(RpcMessage rpcRequest)
        {
            var serializeBuf = new Common.SerializeBuffer();
            serializeBuf.ConcatenateBuff(rpcRequest.RequestContent);
            var itEnd = serializeBuf.End();
            var it = serializeBuf.Begin();
            if (it.Equals(itEnd))
            {
                return null;
            }
            var requestType = (PineHillRSLRpcRequest.RequestType)BitConverter.ToInt32(it.DataBuff, it.RecordOff);
            it = it.Next();
            if (it.Equals(itEnd))
            {
                return null;
            }
            var messageContent = new byte[it.RecordSize];
            Buffer.BlockCopy(it.DataBuff, it.RecordOff, messageContent, 0, it.RecordSize);
            switch (requestType)
            {
                case PineHillRSLRpcRequest.RequestType.InsertTable:
                    return Common.Serializer<InsertTableRequest>.Deserialize(messageContent);
                case PineHillRSLRpcRequest.RequestType.ReadTable:
                    return Common.Serializer<ReadTableRequest>.Deserialize(messageContent);
                case PineHillRSLRpcRequest.RequestType.Unknown:
                default:
                    return null;
            }
        }

        public static RpcMessage CreateRpcMessage(PineHillRSLRpcRequest rpcRequest)
        {
            var rpcMsg = new RpcMessage();
            var serializeBuf = new Common.SerializeBuffer();
            var messageTypeData = BitConverter.GetBytes((int)rpcRequest.Type);
            var messageContentData = rpcRequest.Serialize();
            var dataList = new List<byte[]>();
            dataList.Add(messageTypeData);
            dataList.Add(messageContentData);
            serializeBuf.AppendBlocks(dataList);
            rpcMsg.RequestContent = serializeBuf.DataBuf;

            return rpcMsg;
        }
    }


    public class PineHillRSLClient
    {
        private RpcClient _rpcClient = new RpcClient();
        private List<NodeAddress> _svrAddrList = new List<NodeAddress>();

        public PineHillRSLClient(List<string> serviceSvrList)
        {
            if (serviceSvrList == null)
            {
                return;
            }
            foreach(var svrAddrStr in serviceSvrList)
            {
                var nodeAddr = NodeAddress.DeSerialize(svrAddrStr);
                if (nodeAddr != null)
                {
                    _svrAddrList.Add(nodeAddr);
                }
            }
        }

        public async Task InsertTable(string key, string value)
        {
            // get server addr
            var svrAddr = GetServerAddr();
            var rpcRequest = new InsertTableRequest(key, value);
            var rpcMsg = PineRequetMessageFactory.CreateRpcMessage(rpcRequest);
            await _rpcClient.SendRequest(svrAddr, rpcMsg);
        }

        public async Task<string> ReadTable(string key)
        {
            // get server addr
            var svrAddr = GetServerAddr();
            var rpcRequest = new ReadTableRequest(key);
            var rpcMsg = PineRequetMessageFactory.CreateRpcMessage(rpcRequest);
            var respMsg = await _rpcClient.SendRequest(svrAddr, rpcMsg);
            var readResp = PineRequetMessageFactory.CreateRpcRequest(respMsg) as ReadTableRequest;
            return readResp.Value;
        }


        private NodeAddress GetServerAddr()
        {
            if (_svrAddrList != null && _svrAddrList.Count > 0)
            {
                return _svrAddrList[0];
            }

            return new NodeAddress(new NodeInfo("127.0.0.1"), 1000);
        }
    }


}
