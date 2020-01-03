﻿using Paxos.Message;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Message
{
    /// <summary>
    /// Paxos message deliver interface
    /// </summary>
    public interface IMessageDeliver : IDisposable
    {
        ///
        /// Following are messages channel, should be moved out of the node interface
        ///
        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>

        Task DeliverMessage(PaxosMessage message);
    }
}
