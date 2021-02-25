using PineHillRSL.Paxos.Message;
using PineHillRSL.Paxos.Protocol;
using System;
using System.Threading.Tasks;

namespace PineHillRSL.Paxos.Message
{
    /// <summary>
    /// Deliver the paxos message to roles who will handle it
    /// </summary>
    public class PaxosNodeMessageDeliver : IMessageDeliver
    {
        private ProposerRole _proposerRole;
        private VoterRole _voterRole;

        public PaxosNodeMessageDeliver(ProposerRole proposerRole, VoterRole voterRole)
        {
            if (proposerRole == null)
            {
                throw new ArgumentNullException("proposerrole");
            }
            if (voterRole == null)
            {
                throw new ArgumentNullException("voterrole");
            }
            _proposerRole = proposerRole;
            _voterRole = voterRole;
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
        public async Task DeliverMessage(PaxosMessage message)
        {
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
            }
        }

        public async Task<PaxosMessage> Request(PaxosMessage request)
        {
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
            return resp;
        }

    }
}
