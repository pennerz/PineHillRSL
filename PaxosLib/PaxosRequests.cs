using System;
using System.Collections.Generic;
using System.Text;
using Paxos.Common;

namespace Paxos.Request
{
    /// <summary>
    /// Paxos decree
    /// </summary>
    [Serializable()]
    public class PaxosDecree : ISer
    {
        public PaxosDecree() { }
        public PaxosDecree(string decree)
        {
            Content = decree;
        }
        public string Content;

        public string Serialize()
        {
            return Content;
        }

        public void DeSerialize(string str)
        {
            Content = str;
        }
    }

    /// <summary>
    /// Decree read result
    /// </summary>
    public class DecreeReadResult
    {
        public bool IsFound { get; set; }
        public ulong MaxDecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
    }

    /// <summary>
    /// Decree propose result
    /// </summary>
    public class ProposeResult
    {
        public ulong DecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
        public TimeSpan CollectLastVoteTimeInMs { get; set; }
        public TimeSpan VoteTimeInMs { get; set; }
        public TimeSpan CommitTimeInMs { get; set; }

        public TimeSpan GetProposeCostTime { get; set; }
        public TimeSpan GetProposeLockCostTime { get; set; }
        public TimeSpan PrepareNewBallotCostTime { get; set; }
        public TimeSpan BroadcastQueryLastVoteCostTime { get; set; }

    }
}
