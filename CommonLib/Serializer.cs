using System;
using System.IO;
using System.Xml.Serialization;

namespace Paxos.Common
{
    public class Serializer<T>
        where T : class, new()
    {
        public static string Serialize(T val)
        {
            var xmlSerializer = new XmlSerializer(typeof(T));
            var stream = new MemoryStream();
            xmlSerializer.Serialize(stream, val);
            var data = stream.GetBuffer();
            return Base64Encode(data);
        }

        public static T Deserialize(string str)
        {
            var data = Base64Decode(str);

            var stream = new MemoryStream(data);
            var xmlSerializer = new XmlSerializer(typeof(T));
            return xmlSerializer.Deserialize(stream) as T;
        }

        private static string Base64Encode(byte[] data)
        {
            return System.Convert.ToBase64String(data);
        }
        private static byte[] Base64Decode(string base64EncodedData)
        {
            return System.Convert.FromBase64String(base64EncodedData);
        }
    }

}
