﻿using System;

namespace Paxos.PerCounter
{
    public enum  NetworkPerfCounterType
    {
        Base = 10000,
        RpcConnectionLockWaitTime,
        NetworkMessageProcessTaskCreationTime,
        NetworkMessageRecvWaitTime,
        NetworkMessageBatchCount,
        NetworkMessageProcessTime,
        ConcurrentNetworkTaskCount,
    }
}