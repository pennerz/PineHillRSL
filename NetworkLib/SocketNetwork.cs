using PineRSL.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace PineRSL.Network
{

    class NetworkMessageSerializer
    {
        public static byte[] Serialize(NetworkMessage msg)
        {
            msg.ActivityId.ToByteArray();

            var serializerBuffer = new SerializeBuffer();
            serializerBuffer.AppendBlock(msg.ActivityId.ToByteArray());
            serializerBuffer.AppendBlock(BitConverter.GetBytes(msg.DeliveredTime.Ticks));
            serializerBuffer.AppendBlock(BitConverter.GetBytes(msg.ReceivedTime.Ticks));
            serializerBuffer.AppendBlock(msg.Data);
            return serializerBuffer.DataBuf;
        }

        public static NetworkMessage DeSerialize(byte[] data)
        {
            var serializerBuffer = new SerializeBuffer();
            serializerBuffer.ConcatenateBuff(data);

            var msg = new NetworkMessage();
            var blockIt = serializerBuffer.Begin();
            msg.ActivityId = new Guid(blockIt.DataBuff);
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            msg.DeliveredTime = new DateTime(BitConverter.ToInt64(blockIt.DataBuff));
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            msg.ReceivedTime = new DateTime(BitConverter.ToInt64(blockIt.DataBuff));
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            msg.Data = blockIt.DataBuff;

            return msg;
        }
    }
    class TcpPacket
    {
        byte[] _packBuffer;
        byte[] _dataBuffer;
        int _dataSize;
        public TcpPacket(byte[] packBuffer, byte[] dataBuffer)
        {
            _packBuffer = packBuffer;
            _dataBuffer = dataBuffer;
            _dataSize = dataBuffer.Length;
        }

        public TcpPacket(byte[] packBuffer)
        {
            _packBuffer = packBuffer;
            var bufferSerializer = new SerializeBuffer();
            bufferSerializer.ConcatenateBuff(packBuffer);
            var itBlock = bufferSerializer.Begin();
            if (itBlock == bufferSerializer.End())
            {
                return;
            }

            _dataBuffer = itBlock.DataBuff;
            _dataSize = _dataBuffer.Length;
        }

        public int DataSize => _dataSize;
        public byte[] Data => _dataBuffer;
        public byte[] PacketData => _packBuffer;
    }
    class NetworkMessageBuilder
    {
        public static TcpPacket BuildTcpPacket(NetworkMessage msg)
        {
            var data = NetworkMessageSerializer.Serialize(msg);
            var serializerBuffer = new SerializeBuffer();
            serializerBuffer.AppendBlock(data);
            return new TcpPacket(serializerBuffer.DataBuf, data);
        }

        public static NetworkMessage BuildNetworkMessage(TcpPacket tcpPacket)
        {
            return NetworkMessageSerializer.DeSerialize(tcpPacket.Data);
        }
    }

    /// <summary>
    /// Network connection interface
    /// </summary>
    public class SocketConnection : IConnection
    {
        TcpClient _tcpConnection;
        NodeAddress _remoteAddress;
        NodeAddress _localAddress;

        NodeAddress BuildNodeAddress(IPEndPoint endpoint)
        {
            var nodeInfo = new NodeInfo(endpoint.Address.ToString());
            return new NodeAddress(nodeInfo, endpoint.Port);
        }

        public SocketConnection(TcpClient tcpClient)
        {
            _tcpConnection = tcpClient;
            var remoteEndpoint = (IPEndPoint)_tcpConnection.Client.RemoteEndPoint;
            var localEndpoint = (IPEndPoint)_tcpConnection.Client.LocalEndPoint;
            _remoteAddress = BuildNodeAddress(remoteEndpoint);
            _localAddress = BuildNodeAddress(localEndpoint);
        }

        public NodeAddress RemoteAddress => _remoteAddress;

        public NodeAddress LocalAddress => _localAddress;

        public async Task SendMessage(NetworkMessage msg)
        {
            var networkStream = _tcpConnection.GetStream();

            var tcpPack = NetworkMessageBuilder.BuildTcpPacket(msg);

            // wrap network message into a pack
            await networkStream.WriteAsync(tcpPack.PacketData, 0, tcpPack.PacketData.Length);
        }

        public async Task<List<NetworkMessage>> ReceiveMessage()
        {
            var result = new List<NetworkMessage>();
            var networkStream = _tcpConnection.GetStream();
            byte[] packBuffer = new byte[4096 * 1024 + 1024];

            do
            {
                int readDataSize = 0;
                var readBlockSize = await networkStream.ReadAsync(packBuffer, 0, sizeof(int) - readDataSize);
                readDataSize += readBlockSize;
                if (readDataSize != sizeof(int))
                {
                    return result;
                }
                int dataSize = BitConverter.ToInt32(packBuffer, 0);
                readDataSize = 0;
                do
                {
                    readBlockSize = await networkStream.ReadAsync(packBuffer, sizeof(int) + readDataSize, dataSize - readDataSize);
                    readDataSize += readBlockSize;
                } while (readDataSize < dataSize);

                var tcpPack = new TcpPacket(packBuffer);
                var msg = NetworkMessageBuilder.BuildNetworkMessage(tcpPack);
                result.Add(msg);

            } while (networkStream.CanRead);

            return result;
        }
    }

    /// <summary>
    /// Network server interface.
    /// </summary>
    public class TcpServer : INetworkServer
    {
        TcpListener _server;
        Task _listenTask;
        IConnectionChangeNotification _connectionChangeNotifier;

        public Task StartServer(NodeAddress serverAddr)
        {
            Int32 port = 13000;
            IPAddress localAddr = IPAddress.Parse("127.0.0.1");

            _server = new TcpListener(localAddr, port);
            _server.Start();
            _listenTask = Task.Run(async () =>
            {
                do
                {
                    var tcpClient = await _server.AcceptTcpClientAsync();

                    // create a new connection
                    if (_connectionChangeNotifier != null)
                    {
                        var newConnect = new SocketConnection(tcpClient);
                        _connectionChangeNotifier.OnNewConnection(newConnect);
                    }

                } while (true);

            });

            return Task.CompletedTask;
        }

        public async Task StopServer()
        {
            await _listenTask;
        }

        public void SubscribeConnectionChangeNotification(IConnectionChangeNotification notifier)
        {
            _connectionChangeNotifier = notifier;
        }
    }

    /// <summary>
    /// Creator for create network objects. Different implementation should implment
    /// a INetworkCreator too so to create the objects.
    /// </summary>
    public class TcpNetworkCreator : INetworkCreator
    {
        public Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {
            var server = new PineRSL.Network.TcpServer();
            return Task.FromResult((INetworkServer)server);
        }

        public async Task<IConnection> CreateNetworkClient(NodeAddress serverAddr)
        {
            var tcpClient = new TcpClient();
            IPAddress serverIp = IPAddress.Parse(serverAddr.Node.Name);
            await tcpClient.ConnectAsync(serverIp, serverAddr.Port);

            var clientConnection = new SocketConnection(tcpClient);
            return clientConnection;
        }
    }


}
