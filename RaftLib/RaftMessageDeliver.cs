using PineHillRSL.Raft.Message;
using System;
using System.Threading.Tasks;

namespace PineHillRSL.Raft.Message
{
    /// <summary>
    /// Deliver the paxos message to roles who will handle it
    /// </summary>
    public class RaftNodeMessageDeliver : IMessageDeliver
    {
        public RaftNodeMessageDeliver()
        {
        }

        public virtual void Dispose()
        {

        }

        ///
        /// Following are messages channel, should be moved out of the node interface
        ///
        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        public async Task DeliverMessage(RaftMessage message)
        {/*
            switch (message.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                case PaxosMessageType.BEGINBALLOT:
                //case PaxosMessageType.SUCCESS:
                    await _voterRole.HandlePaxosMessage(message);
                    break;
                case PaxosMessageType.LASTVOTE:
                case PaxosMessageType.VOTE:
                case PaxosMessageType.SUCCESS:
                case PaxosMessageType.STALEBALLOT:
                    await _proposerRole.HandlePaxosMessage(message);
                    break;
                default:
                    break;
            }*/
        }

        public async Task<RaftMessage> Request(RaftMessage request)
        {
            /*
            PaxosMessage resp = null;
            switch (request.MessageType)
            {
                case PaxosMessageType.CheckpointSummaryReq:
                case PaxosMessageType.CheckpointDataReq:
                    {
                        resp = await _proposerRole.HandleRequest(request);
                        break;
                    }
            }
            if (resp != null)
            {
                resp.SourceNode = request.TargetNode;
                resp.TargetNode = request.SourceNode;
            }
            return resp;*/
            return null;
        }

    }
}
