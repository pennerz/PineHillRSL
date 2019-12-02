using Paxos.Message;
using Paxos.Protocol;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.MessageDelivery
{
    public class PaxosNodeMessageDelivery : IMessageDelivery
    {
        private ProposerRole _proposerRole;
        private VoterRole _voterRole;

        public PaxosNodeMessageDelivery(ProposerRole proposerRole, VoterRole voterRole)
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
                    await _voterRole.DeliverNextBallotMessage(message as NextBallotMessage);
                    break;
                case PaxosMessageType.LASTVOTE:
                    await _proposerRole.DeliverLastVoteMessage(message as LastVoteMessage);
                    break;
                case PaxosMessageType.BEGINBALLOT:
                    await _voterRole.DeliverBeginBallotMessage(message as BeginBallotMessage);
                    break;
                case PaxosMessageType.VOTE:
                    await _proposerRole.DeliverVoteMessage(message as VoteMessage);
                    break;
                case PaxosMessageType.SUCCESS:
                    await _voterRole.DeliverSuccessMessage(message as SuccessMessage);
                    break;
                case PaxosMessageType.STALEBALLOT:
                    await _proposerRole.DeliverStaleBallotMessage(message as StaleBallotMessage);
                    break;
                default:
                    break;
            }
        }

    }
}
