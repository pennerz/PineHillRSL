using System;
using System.Collections.Generic;
using System.Text;

namespace Paxos.Request
{
    [Serializable()]
    public class PaxosDecree
    {
        public string Content;
    }

    public class DecreeReadResult
    {
        public bool IsFound { get; set; }
        public ulong MaxDecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
    }

    public class ProposeResult
    {
        public ulong DecreeNo { get; set; }
        public PaxosDecree Decree { get; set; }
    }

}
