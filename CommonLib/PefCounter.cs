using System;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;


namespace PineHillRSL.Common
{
    public class PerfCounterManager
    {
        class Counter
        {
            private Int64 _value = 0;
            private int _count = 0;
            private Int64 _maxvalue = 0;

            public Counter()
            {
            }

            public Int64 Value => _value;

            public Int64 MaximumValue => _maxvalue;

            public int Count => _count;

            public Int64 Accumulate(Int64 val)
            {
                Interlocked.Increment(ref _count);
                var result = Interlocked.Add(ref _value, val);
                if (val > _maxvalue)
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

        public Int64 Accumulate(int counterType, Int64 counterValue)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            return counter.Accumulate(counterValue);
        }

        public Int64 GetCounterValue(int counterType)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            return counter.Value;
        }
        public Int64 GetCounterAvgValue(int counterType)
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
        public Int64 GetCounterMaximuValue(int counterType)
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
            return counter.MaximumValue;
        }

        public Int64 GetCounterCount(int counterType)
        {
            var counter = GetCounter(counterType);
            if (counter == null)
            {
                return 0;
            }
            return counter.Count;
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
        public static void ReportCounter(int counterType, Int64 value)
        {
            PerfCounterManager.GetInst().Accumulate(counterType, value);
        }

        public static Int64 GetCounterCount(int counterType)
        {
            return PerfCounterManager.GetInst().GetCounterCount(counterType);
        }

        public static Int64 GetCounterSum(int counterType)
        {
            return PerfCounterManager.GetInst().GetCounterValue(counterType);
        }
        public static Int64 GetCounterAvg(int counterType)
        {
            return PerfCounterManager.GetInst().GetCounterAvgValue(counterType);
        }
        public static Int64 GetMaximumValue(int conterType)
        {
            return PerfCounterManager.GetInst().GetCounterMaximuValue(conterType);
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
