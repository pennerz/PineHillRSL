using System;

namespace PaxosLib
{
    using System.Threading.Tasks;

    public class PaxosDecree
    {

    }

    public enum PaxosMessageType
    {
        NEXTBALLOT,
        LASTVOTE,
        BEGINBALLOT,
        VOTE,
        SUCCESS
    }
    public class PaxosMessage
    {
        public PaxosMessageType MessageType {get; set;}
        public string SourceNode { get; set; }
        public string TargetNode { get; set; }
        public UInt64 DecreeNo { get; set; }
        public UInt64 BallotNo { get; set; }
    }

    public class NextBallotMessage : PaxosMessage
    {

    }

    public class LastVoteMessage : PaxosMessage
    {

    }

    public class BeginBallotMessage : PaxosMessage
    { }

    public class VoteMessage : PaxosMessage
    {

    }

    public class SuccessMessage : PaxosMessage
    {

    }


    public interface IPaxos
    {
        Task UpdateSuccessfullDecree(PaxosDecree decree);
        Task Checkpoint();
    }

    public interface IPaxosStateMachine
    {
    }
    public interface IPaxosNodeTalkChannel
    {
        Task SendMessage(PaxosMessage msg);
    }

    public class Ledger
    {
        public UInt64 NextBallot { get; set; }
        public UInt64 LastVote { get; set; }

    }

    public class PaxosNode
    {
        IPaxosNodeTalkChannel nodeTalkChannel;

        public PaxosNode(IPaxosNodeTalkChannel nodeTalkChannel)
        {
            if (nodeTalkChannel == null)
            {
                throw new ArgumentNullException("Node talk channel is null");
            }

            this.nodeTalkChannel = nodeTalkChannel;
        }

        public Task ProposeDecree(PaxosDecree decree)
        {
            return Task.CompletedTask;
        }

        public Task DeliverMessage(PaxosMessage message)
        {
            switch (message.MessageType)
            {
                case PaxosMessageType.NEXTBALLOT:
                    return ProcessNextBallot(message as NextBallotMessage);
                case PaxosMessageType.LASTVOTE:
                    return ProcessLastVote(message as LastVoteMessage);
                case PaxosMessageType.BEGINBALLOT:
                    return ProcessBeginBallot(message as BeginBallotMessage);
                case PaxosMessageType.VOTE:
                    return ProcessVote(message as VoteMessage);
                case PaxosMessageType.SUCCESS:
                    return ProcessSuccess(message as SuccessMessage);
                default:
                    return Task.CompletedTask;
            }
        }

        private Task ProcessNextBallot(NextBallotMessage msg)
        {
            return Task.CompletedTask;
        }
        private Task ProcessLastVote(LastVoteMessage msg)
        {
            return Task.CompletedTask;
        }
        private Task ProcessBeginBallot(BeginBallotMessage msg)
        {
            return Task.CompletedTask;
        }
        private Task ProcessVote(VoteMessage msg)
        {
            return Task.CompletedTask;
        }
        private Task ProcessSuccess(SuccessMessage msg)
        {
            return Task.CompletedTask;
        }
    }

}
