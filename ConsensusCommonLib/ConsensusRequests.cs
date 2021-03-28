using PineHillRSL.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace PineHillRSL.Consensus.Request
{
    /// <summary>
    /// Paxos decree
    /// </summary>
    [Serializable()]
    public class ConsensusDecree : ISer
    {
        private byte[] _data = null;
        public ConsensusDecree() { }
        public ConsensusDecree(byte[] decree)
        {
            _data = decree;
        }
        public ConsensusDecree(string decree)
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
        public ConsensusDecree Decree { get; set; }
    }


    /// <summary>
    /// Decree propose result
    /// </summary>
    public class ProposeResult
    {
        // common part
        public ConsensusDecree Decree { get; set; }

        // paxos part
        public ulong DecreeNo { get; set; }
        public ulong CheckpointedDecreeNo { get; set; }

        public TimeSpan CollectLastVoteTimeInMs { get; set; }
        public TimeSpan VoteTimeInMs { get; set; }
        public TimeSpan CommitTimeInMs { get; set; }

        public TimeSpan GetProposeCostTime { get; set; }
        public TimeSpan GetProposeLockCostTime { get; set; }
        public TimeSpan PrepareNewBallotCostTime { get; set; }
        public TimeSpan BroadcastQueryLastVoteCostTime { get; set; }


        // raft part
        public ulong LogIndex { get; set; }
        public string Leader { get; set; }
        public bool Retry { get; set; }

    }
}
