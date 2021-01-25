using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace PineHillRSL.Paxos.Request
{
    /// <summary>
    /// Paxos decree
    /// </summary>
    [Serializable()]
    public class PaxosDecree : ISer
    {
        private byte[] _data = null;
        public PaxosDecree() { }
        public PaxosDecree(byte[] decree)
        {
            _data = decree;
        }
        public PaxosDecree(string decree)
        {
            _data = Encoding.UTF8.GetBytes(decree);
        }

        public string Content 
        {
            get
            {
                if (_data == null)
                {
                    return "";
                }
                return Encoding.UTF8.GetString(_data);
            }
            set
            {
                _data = Encoding.UTF8.GetBytes(value);
            }
        }

        public byte[] Data
        {
            get
            {
                return _data;
            }
            set
            {
                _data = value;
            }
        }

        public byte[] Serialize()
        {
            return _data;
        }

        public void DeSerialize(byte[] str)
        {
            _data = str;
        }
    }

    /// <summary>
    /// Decree read result
    /// </summary>
    public class DecreeReadResult
    {
        public bool IsFound { get; set; }
        public ulong MaxDecreeNo { get; set; }
        public ulong CheckpointedDecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
    }

    /// <summary>
    /// Decree propose result
    /// </summary>
    public class ProposeResult
    {
        public ulong DecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
        public ulong CheckpointedDecreeNo { get; set; }

        public TimeSpan CollectLastVoteTimeInMs { get; set; }
        public TimeSpan VoteTimeInMs { get; set; }
        public TimeSpan CommitTimeInMs { get; set; }

        public TimeSpan GetProposeCostTime { get; set; }
        public TimeSpan GetProposeLockCostTime { get; set; }
        public TimeSpan PrepareNewBallotCostTime { get; set; }
        public TimeSpan BroadcastQueryLastVoteCostTime { get; set; }

    }
}
