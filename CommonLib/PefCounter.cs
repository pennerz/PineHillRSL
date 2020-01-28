using System;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;


namespace Paxos.Common
{
    public class PerfCounterManager
    {
        class Counter
        {
            private int _value = 0;
            private int _count = 0;
            private int _maxvalue = 0;

            public Counter()
            {
            }

            public int Value => _value;

            public int Count => _count;

            public int Accumulate(int val)
            {
                Interlocked.Increment(ref _count);
                var result = Interlocked.Add(ref _value, val);
                if (result > _maxvalue)
                {
                    _maxvalue = result;
                }
                return result;
            }
        }

        private static readonly PerfCounterManager _inst = new PerfCounterManager();

        ConcurrentDictionary<int, Counter> _counters = new ConcurrentDictionary<int, Counter>();

        public static PerfCounterManager GetInst()
        {
            return _inst;
        }

        public PerfCounterManager()
        { }

        public int Accumulate(int counterType, int counterValue)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            return counter.Accumulate(counterValue);
        }

        public int GetCounterValue(int counterType)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            return counter.Value;
        }
        public int GetCounterAvgValue(int counterType)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            if (counter.Count == 0)
            {
                return 0;
            }
            return counter.Value / counter.Count;
        }

        private Counter GetCounter(int counterType)
        {
            Counter counter = null;
            if (!_counters.TryGetValue(counterType, out counter))
            {
                counter = new Counter();
                if (!_counters.TryAdd(counterType, counter))
                {
                    if (!_counters.TryGetValue(counterType, out counter))
                    {
                        return null;
                    }
                }
            }
            return counter;
        }
    }



    public class PerfTimerCounter : IDisposable
    {
        private DateTime _begin;
        private int _counterType;

        public PerfTimerCounter(int counterType)
        {
            _counterType = counterType;
            _begin = DateTime.Now;
        }

        public void Dispose()
        {
            PerfCounterManager.GetInst().Accumulate(_counterType, (int)(DateTime.Now - _begin).TotalMilliseconds);
        }
    }

    public class StatisticCounter
    {
        public static void ReportCounter(int counterType, int value)
        {
            PerfCounterManager.GetInst().Accumulate(counterType, value);
        }

        public static int GetCounterSum(int counterType)
        {
            return PerfCounterManager.GetInst().GetCounterValue(counterType);
        }
        public static int GetCounterAvg(int counterType)
        {
            return PerfCounterManager.GetInst().GetCounterAvgValue(counterType);
        }
    }

    public class Statistic
    {
        private double _min = UInt64.MaxValue;
        private double _max = 0;
        private double _sum = 0;
        private UInt64 _count = 0;
        private ConcurrentDictionary<int, int> _lock = new ConcurrentDictionary<int, int>();

        public double Min => _min;
        public double Max => _max;
        public double Avg
        {
            get
            {
                lock (_lock)
                {
                    if (_count > 0)
                    {
                        return _sum / _count;
                    }
                    return _min;
                }
            }
        }

        public UInt64 Count { get; set; }

        public void Accumulate(double val)
        {
            lock (_lock)
            {
                if (val < _min)
                {
                    _min = val;
                }
                if (val > _max)
                {
                    _max = val;
                }
                _sum += val;
                _count++;
            }
        }
    }


}
