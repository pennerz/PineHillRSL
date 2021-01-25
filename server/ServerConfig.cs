using System;
using System.Collections.Generic;
using System.Text;

namespace PineHillRSL.Server
{
    class ServerConfig
    {
        public List<string> RSLClusterServers { get; set; }
        public string RSLServerAddr { get; set; }
        public string ServiceServerAddr { get; set; }
    }
}
