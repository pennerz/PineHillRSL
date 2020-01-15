using System;
using System.IO;
using System.Xml.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Paxos.Common
{

    public interface ISer
    {
        string Serialize();
        void DeSerialize(string str);
    }
    public class Serializer<T>
        where T : class, ISer, new()
    {
        //static XmlSerializer _xmlSerializer = new XmlSerializer(typeof(T));
        public static string Serialize(T val)
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

        public static T Deserialize(string str)
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
