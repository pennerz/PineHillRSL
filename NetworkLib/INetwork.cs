using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Paxos.Network
{
    public class NodeInfo : IEquatable<NodeInfo>
    {
        private readonly string _name;
        public bool Equals(NodeInfo rhs)
        {
            return Name.Equals(rhs.Name);
        }
        public override bool Equals(object obj)
        {
            return Equals(obj as NodeInfo);
        }
        public override int GetHashCode()
        {
            return _name.GetHashCode();
        }
        public NodeInfo(string name)
        {
            _name = name;
        }
        public string Name => _name;
    }

    /// <summary>
    /// node address
    /// </summary>
    public class NodeAddress : IEquatable<NodeAddress>
    {
        private readonly NodeInfo _nodeInfo;
        private readonly ushort _port;

        public bool Equals(NodeAddress rhs)
        {
            if (!Node.Equals(rhs.Node))
            {
                return false;
            }
            if (Port != rhs.Port)
            {
                return false;
            }
            return true;
        }
        public override bool Equals(object obj)
        {
            return Equals(obj as NodeAddress);
        }

        public override int GetHashCode()
        {
            return Node.GetHashCode() + Port.GetHashCode();
        }

        public NodeAddress(NodeInfo nodeInfo, ushort port)
        {
            _nodeInfo = nodeInfo;
            _port = port;
        }

        public NodeInfo Node => _nodeInfo;
        public ushort Port => _port;
    }

    /// <summary>
    /// Network communication message
    /// </summary>
    public class NetworkMessage
    {
        public NetworkMessage()
        {
        }

        public Guid ActivityId { get; set; }
        public string Data { get; set; }
    }

    /// <summary>
    /// Network connection interface
    /// </summary>
    public interface IConnection
    {
        NodeAddress RemoteAddress { get; }

        NodeAddress LocalAddress { get; }

        Task SendMessage(NetworkMessage msg);

        Task<List<NetworkMessage>> ReceiveMessage();
    }

    /// <summary>
    /// Interface to notify connection changing, which is used by server.
    /// </summary>
    public interface IConnectionChangeNotification
    {
        void OnNewConnection(IConnection newConnection);
    }

    /// <summary>
    /// Network server interface.
    /// </summary>
    public interface INetworkServer
    {
        Task StartServer(NodeAddress serverAddr);
        Task StopServer();
        void SubscribeConnectionChangeNotification(IConnectionChangeNotification notifier);
    }

    /// <summary>
    /// Creator for create network objects. Different implementation should implment
    /// a INetworkCreator too so to create the objects.
    /// </summary>
    public interface INetworkCreator
    {
        Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr);
        Task<IConnection> CreateNetworkClient(NodeAddress localAddrr, NodeAddress serverAddr);
    }

    /// <summary>
    /// Network objects factory.
    /// </summary>
    public class NetworkFactory
    {
        private static INetworkCreator _creator = null;

        public static void SetNetworkCreator(INetworkCreator creator)
        {
            _creator = creator;
        }
        public static async Task<INetworkServer> CreateNetworkServer(NodeAddress serverAddr)
        {
            if (_creator != null)
            {
                var networkServer = await _creator.CreateNetworkServer(serverAddr);
                return networkServer;
            }
            return null;
        }

        public static async Task<IConnection> CreateNetworkClient(NodeAddress localAddr, NodeAddress serverAddr)
        {
            if (_creator != null)
            {
                var connection = await _creator.CreateNetworkClient(localAddr, serverAddr);
                return connection;
            }
            return null;
        }
    }

}
