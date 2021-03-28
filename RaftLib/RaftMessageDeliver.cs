using PineHillRSL.Raft.Message;
using PineHillRSL.Raft.Protocol;
using System;
using System.Threading.Tasks;

namespace PineHillRSL.Raft.Message
{
    /// <summary>
    /// Deliver the paxos message to roles who will handle it
    /// </summary>
    public class RaftNodeMessageDeliver : IMessageDeliver
    {
        RaftRole _raftRole;
        public RaftNodeMessageDeliver(RaftRole raftRole)
        {
            _raftRole = raftRole;
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
        {
            switch (message.MessageType)
            {
                case RaftMessageType.AppendEntityReq:
                case RaftMessageType.VoteReq:
                //case PaxosMessageType.SUCCESS:
                    await _raftRole.HandleRequest(message);
                    break;
                case RaftMessageType.AppendEntityResp:
                case RaftMessageType.VoteResp:
                    //await _proposerRole.HandlePaxosMessage(message);
                    break;
                default:
                    break;
            }
        }

        public async Task<RaftMessage> Request(RaftMessage request)
        {
            RaftMessage resp = null;
            switch (request.MessageType)
            {
                case RaftMessageType.AppendEntityReq:
                case RaftMessageType.VoteReq:
                    //case PaxosMessageType.SUCCESS:
                    resp = await _raftRole.HandleRequest(request);
                    break;
                case RaftMessageType.AppendEntityResp:
                case RaftMessageType.VoteResp:
                    //await _proposerRole.HandlePaxosMessage(message);
                    break;
                default:
                    break;
            }
            if (resp != null)
            {
                resp.SourceNode = request.TargetNode;
                resp.TargetNode = request.SourceNode;
            }

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
            return resp;
        }

    }
}
