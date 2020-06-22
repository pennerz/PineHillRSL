using System;
using System.IO;
using System.Xml.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace Paxos.Common
{
    public class SerializeBufferIterator : IEquatable<SerializeBufferIterator>
    {
        public enum IteratorType
        {
            Normal,
            End
        };

        private IteratorType _itType;
        private byte[] _data = null;
        private int _recordOff = 0;
        private int _recordSize = 0;

        public SerializeBufferIterator(IteratorType type, byte[] dataBuf, int off)
        {
            _itType = type;
            if (_itType != IteratorType.End)
            {
                _data = dataBuf;
                _recordOff = off;
                _recordSize = BitConverter.ToInt32(dataBuf, _recordOff);
            }
        }

        public SerializeBufferIterator Next()
        {
            int blockSize = BitConverter.ToInt32(_data, _recordOff);
            int nextOff = _recordOff + sizeof(int) + blockSize;
            if (nextOff >= _data.Length)
            {
                return new SerializeBufferIterator(IteratorType.End, null, 0);
            }
            return new SerializeBufferIterator(IteratorType.Normal, _data, nextOff);
        }

        public bool Equals(SerializeBufferIterator rhs)
        {
            if (rhs == null)
            {
                return false;
            }
            if (_itType == rhs._itType && _itType == IteratorType.End)
            {
                return true;
            }
            if (_itType != rhs._itType)
            {
                return false;
            }
            if (_data != rhs._data)
            {
                return false;
            }
            if (_recordOff != rhs._recordOff)
            {
                return false;
            }


            return true;
        }

        public override bool Equals(object obj)
        {
            var rhs = obj as SerializeBufferIterator;
            return Equals(rhs);
        }

        public override int GetHashCode()
        {
            return 1;
        }

        public byte[] DataBuff => _data;
        public int RecordOff => _recordOff + sizeof(int);
        public int RecordSize => _recordSize;
    }

    public class SerializeBuffer
    {
        private byte[] _dataBuf = null;

        public SerializeBuffer()
        {

        }

        public void AppendBlocks(List<byte[]> scatteredData)
        {
            int dataSize = 0;
            foreach(var dataBuf in scatteredData)
            {
                if(dataBuf == null)
                {
                    continue;
                }
                dataSize += dataBuf.Length + sizeof(int);
            }
            if (_dataBuf != null)
            {
                dataSize += _dataBuf.Length;
            }
            var data = new byte[dataSize];
            if (_dataBuf != null)
            {
                Buffer.BlockCopy(_dataBuf, 0, data, 0,  _dataBuf.Length);
            }
            int off = 0;
            if (_dataBuf != null)
            {
                off = _dataBuf.Length;
            }
            _dataBuf = data;


            int blockSize = 0;
            foreach(var dataBuf in scatteredData)
            {
                if (dataBuf == null)
                {
                    continue;
                }
                blockSize = dataBuf.Length;
                Buffer.BlockCopy(BitConverter.GetBytes(blockSize), 0, data, off, sizeof(int));
                off += sizeof(int);
                Buffer.BlockCopy(dataBuf, 0, data, off, dataBuf.Length);
                off += dataBuf.Length;
            }
        }

        public void AppendBlock(byte[] block)
        {
            if (block == null)
            {
                return;
            }
            int dataSize = block.Length;
            if (_dataBuf != null)
            {
                dataSize += _dataBuf.Length;
            }
            var data = new byte[dataSize];
            if (_dataBuf != null)
            {
                Buffer.BlockCopy(_dataBuf, 0, data, 0, _dataBuf.Length);
            }
            int off = 0;
            if (_dataBuf != null)
            {
                off = _dataBuf.Length;
            }
            _dataBuf = data;

            int blockSize = block.Length;
            Buffer.BlockCopy(BitConverter.GetBytes(blockSize), 0, data, off, sizeof(int));
        }

        public void ConcatenateBuff(byte[] buf)
        {
            if (_dataBuf != null)
            {
                byte[] newDataBuf = new byte[buf.Length + _dataBuf.Length];
                Buffer.BlockCopy(_dataBuf, 0, newDataBuf, 0, _dataBuf.Length);
                Buffer.BlockCopy(buf, 0, newDataBuf, _dataBuf.Length, buf.Length);
                _dataBuf = newDataBuf;
            }
            else
            {
                _dataBuf = buf;
            }
        }

        public SerializeBufferIterator Begin()
        {
            if (_dataBuf == null)
            {
                return End();
            }

            return new SerializeBufferIterator(SerializeBufferIterator.IteratorType.Normal, _dataBuf, 0);
        }

        public SerializeBufferIterator End()
        {
            return new SerializeBufferIterator(SerializeBufferIterator.IteratorType.End, null, 0);
        }

        public byte[] DataBuf => _dataBuf;
    }

    public interface ISer
    {
        byte[] Serialize();
        void DeSerialize(byte[] data);
    }
    public class Serializer<T>
        where T : class, ISer, new()
    {
        //static XmlSerializer _xmlSerializer = new XmlSerializer(typeof(T));
        public static byte[] Serialize(T val)
        {
            var begin = DateTime.Now;
            //var xmlSerializer = _xmlSerializer;
            //var xmlAllocateCostTime = DateTime.Now - begin;
            //var stream = new MemoryStream();
            //var objectAllocateCostTime = DateTime.Now - begin;
            //var str = JsonSerializer.Serialize<T>(val);
            var str = val.Serialize();
            //_xmlSerializer.Serialize(stream, val);
            //var data = stream.GetBuffer();
            //var serializeCostTime = DateTime.Now - begin/* - objectAllocateCostTime*/;
            //var data = Encoding.ASCII.GetBytes(str);
            //str = Base64Encode(data);
            var totalCostTime = DateTime.Now - begin;
            if (totalCostTime.TotalMilliseconds > 500)
            {
                //Console.WriteLine("serialize too slow");
            }
            return str;
        }

        public static T Deserialize(byte[] str)
        {
            //var data = Base64Decode(str);
            //str = Encoding.ASCII.GetString(data);
            //return JsonSerializer.Deserialize<T>(str);
            var result = new T();
            result.DeSerialize(str);
            return result;

            //var stream = new MemoryStream(data);
            //var xmlSerializer = _xmlSerializer;
            //return _xmlSerializer.Deserialize(stream) as T;*/
        }

        private static string Base64Encode(byte[] data)
        {
            return System.Convert.ToBase64String(data);
        }
        private static byte[] Base64Decode(string base64EncodedData)
        {
            return System.Convert.FromBase64String(base64EncodedData);
        }

        static byte[] GetBytes(string str)
        {
            byte[] bytes = new byte[str.Length * sizeof(char)];
            System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        // Do NOT use on arbitrary bytes; only use on GetBytes's output on the SAME system
        static string GetString(byte[] bytes)
        {
            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }
    }

}
