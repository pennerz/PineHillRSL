using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PineHillRSL.Common
{
    public class SlidingWindow
    {
        private IItemComparer _itemComparer;
        public class Item
        {
            public ulong Seq { get; set; }
            public object Content { get; set; }
        }

        public interface IItemComparer
        {
            bool IsSmaller(object left, object right);
        }

        private SemaphoreSlim _lock = new SemaphoreSlim(1);
        private ulong _baseSeq = 0;
        private ulong _lastReadySeq = 0;
        private SortedList<ulong, Item> _itemList = new SortedList<ulong, Item>();

        private UInt64 _lastPopSeq = 0;
        private object _lastSmallestVaule = null;

        private DateTime _lastPopTime = DateTime.Now;

        public SlidingWindow(ulong baseSeq, IItemComparer itemComparer)
        {
            _baseSeq = baseSeq;
            _lastReadySeq = _baseSeq;
            _itemComparer = itemComparer;
        }

        public void Add(ulong seq, object content)
        {
            var item = new Item() { Seq = seq, Content = content };
            lock(_itemList)
            {
                if (!_itemList.ContainsKey(seq))
                {
                    _itemList.Add(seq, item);
                }
                else
                {
                    _itemList[seq] = item;
                }
            }
        }

        public Item Pop()
        {
            lock (_itemList)
            {
                if (_itemList.Count == 0)
                {
                    return null;
                }

                if (_itemList.Keys[0] < _lastReadySeq + 1)
                {
                    _itemList.RemoveAt(0);
                    return null;
                }

                if (_itemList.Keys[0] == _lastReadySeq + 1)
                {
                    _lastPopSeq = _lastPopSeq + 1;
                    GetSmallestValueInLock(out _lastSmallestVaule);
                    _lastReadySeq += 1;
                    var item = _itemList[_lastReadySeq];
                    _itemList.RemoveAt(0);
                    _lastPopTime = DateTime.Now;
                    return item;
                }

                return null;
            }
        }

        public ulong GetPendingSequenceCount()
        {
            lock(_itemList)
            {
                return (ulong)_itemList.Count;
            }
        }

        public void GetSmallestItem(out UInt64 smallestKey, out object smallestValue)
        {
            lock(_itemList)
            {
                smallestKey = _lastPopSeq + 1;
                smallestValue = _lastSmallestVaule;
            }
        }

        public Task WaitReadyToContinue()
        {
            if (_itemList.Count < 10)
            {
                return Task.CompletedTask;
            }
            return _lock.WaitAsync();
        }

        public DateTime LastPopTime => _lastPopTime;

        public int Count => _itemList.Count;

        public ulong LastPopSeq => _lastPopSeq;

        public ulong SmallestSeqInWindow
        {
            get
            {
                lock (_itemList)
                {
                    if (_itemList.Count == 0)
                    {
                        return 0;
                    }
                    return _itemList[0].Seq;
                }
            }
        }

        private bool GetSmallestValueInLock(out object smallestVal)
        {
            if (_itemComparer == null)
            {
                smallestVal = null;
                return false;
            }
            if (_itemList.Count == 0)
            {
                smallestVal = null;
                return false;
            }
            var smallestObject = _itemList.Values[0].Content;
            foreach (var itm in _itemList.Values)
            {
                if (!_itemComparer.IsSmaller(smallestObject, itm.Content))
                {
                    smallestObject = itm.Content;
                }
            }

            smallestVal = smallestObject;
            return true;
        }
    }

}
