using System;
using System.Collections.Generic;
using System.Text;

namespace Paxos.Request
{
    /// <summary>
    /// Paxos decree
    /// </summary>
    [Serializable()]
    public class PaxosDecree
    {
        public string Content;
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
    }

}
