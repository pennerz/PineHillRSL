using Paxos.Message;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Network
{
    public class NodeInfo
    {
        public string Name { get; set; }

    }

    //TODO: abstract the PaxosMessage
    public interface IMessageTransport
    {
        Task SendMessage(PaxosMessage msg);
        Task<PaxosMessage> ReceiveMessage();
    }

}
