using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace PineHillRSL.Network
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

        public static NetworkMessage DeSerialize(byte[] data, int off, int length)
        {
            var serializerBuffer = new SerializeBuffer();
            serializerBuffer.ConcatenateBuff(data, off, length);

            var msg = new NetworkMessage();
            var blockIt = serializerBuffer.Begin();
            var buf = new byte[blockIt.RecordSize];
            Buffer.BlockCopy(blockIt.DataBuff, blockIt.RecordOff, buf, 0, blockIt.RecordSize);
            msg.ActivityId = new Guid(buf);
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            msg.DeliveredTime = new DateTime(BitConverter.ToInt64(blockIt.DataBuff, blockIt.RecordOff));
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            msg.ReceivedTime = new DateTime(BitConverter.ToInt64(blockIt.DataBuff, blockIt.RecordOff));
            blockIt = blockIt.Next();
            if (blockIt == serializerBuffer.End())
            {
                throw new Exception("Maleformed network message!");
            }
            buf = new byte[blockIt.RecordSize];
            Buffer.BlockCopy(blockIt.DataBuff, blockIt.RecordOff, buf, 0, blockIt.RecordSize);
            msg.Data = buf;

            return msg;
        }
    }
    class TcpPacket
    {
        byte[] _packBuffer;
        int _dataSize;
        int _dataOff;

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

            _dataSize = itBlock.RecordSize;
            _dataOff = itBlock.RecordOff;
        }

        public int DataSize => _dataSize;
        public int DataOff => _dataOff;
        public byte[] PacketData => _packBuffer;
    }
    class NetworkMessageBuilder
    {
        public static TcpPacket BuildTcpPacket(NetworkMessage msg)
        {
            var data = NetworkMessageSerializer.Serialize(msg);
            var serializerBuffer = new SerializeBuffer();
            serializerBuffer.AppendBlock(data);
            return new TcpPacket(serializerBuffer.DataBuf);
        }

        public static NetworkMessage BuildNetworkMessage(TcpPacket tcpPacket)
        {
            return NetworkMessageSerializer.DeSerialize(tcpPacket.PacketData, tcpPacket.DataOff, tcpPacket.DataSize);
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
            byte[] packBuffer = new byte[4096];

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
                if (dataSize > packBuffer.Length - sizeof(int))
                {
                    packBuffer = new byte[sizeof(int) + dataSize];
                    Buffer.BlockCopy(BitConverter.GetBytes(dataSize), 0, packBuffer, 0, sizeof(int));
                }
                readDataSize = 0;
                do
                {
                    readBlockSize = await networkStream.ReadAsync(packBuffer, sizeof(int) + readDataSize, dataSize - readDataSize);
                    readDataSize += readBlockSize;
                } while (readDataSize < dataSize);

                var tcpPack = new TcpPacket(packBuffer);
                var msg = NetworkMessageBuilder.BuildNetworkMessage(tcpPack);
                msg.ReceivedTime = DateTime.Now;
                result.Add(msg);
            } while (networkStream.DataAvailable);

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
            IPAddress localAddr = IPAddress.Parse(serverAddr.Node.Name);
            Int32 port = serverAddr.Port;

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
                        _connectionChangeNotifier.OnConnectionOpened(newConnect);
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
        public async Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {
            var server = new PineHillRSL.Network.TcpServer();
            await server.StartServer(serverAddr);
            return server;
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
