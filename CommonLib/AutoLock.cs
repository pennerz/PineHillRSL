using System;
using System.Threading;

namespace Paxos.Common
{
    public class AutoLock : IDisposable
    {
        private SemaphoreSlim _lock;
        public AutoLock(SemaphoreSlim ayncLock)
        {
            _lock = ayncLock;
        }

        public void Dispose()
        {
            if (_lock != null)
            {
                _lock.Release();
            }
        }
    }


}
