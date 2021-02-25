using System;

namespace PineHillRSL.PerCounter
{
    public enum  NetworkPerfCounterType
    {
        Base = 10000,
        ProposeTime,
        TableLockWaitTime,
        RpcConnectionLockWaitTime,
        NetworkMessageProcessTaskCreationTime,
        NetworkMessageRecvWaitTime,
        NetworkMessageTransmitTime,
        NetworkMessageRecvDelayedTime,
        NetworkMessageBatchCount,
        NetworkMessageProcessTime,
        ConcurrentNetworkTaskCount,
        CheckpointTableLockTime,
        CheckpointDataSize,
        CheckpointCount,
    }
}