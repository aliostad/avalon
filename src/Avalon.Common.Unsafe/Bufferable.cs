using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Common
{
    public struct Bufferable
    {
        private readonly byte[] _buffer; 
        public Bufferable(byte[] buffer)
        {
            _buffer = buffer;
        }

        public byte[] Buffer => _buffer;

        public static implicit operator Bufferable(string s)
        {
            return new Bufferable(Encoding.UTF8.GetBytes(s));
        }

        public static implicit operator Bufferable(long l)
        {
            return new Bufferable(BitConverter.GetBytes(l));
        }

        public static implicit operator Bufferable(int i)
        {
            return new Bufferable(BitConverter.GetBytes(i));
        }

        public static implicit operator Bufferable(Guid g)
        {
            return new Bufferable(g.ToByteArray());
        }

        public static implicit operator string(Bufferable b)
        {
            return b.Buffer == null || b.Buffer.Length == 0 ? default(string) : Encoding.UTF8.GetString(b.Buffer);
        }

        public static implicit operator long(Bufferable b)
        {
            return b.Buffer == null || b.Buffer.Length == 0 ? default(long) : BitConverter.ToInt64(b.Buffer, 0);
        }

        public static implicit operator int(Bufferable b)
        {
            return b.Buffer == null || b.Buffer.Length == 0 ? default(int) : BitConverter.ToInt32(b.Buffer, 0);
        }

        public static implicit operator Guid(Bufferable b)
        {
            return b.Buffer == null || b.Buffer.Length == 0 ? default(Guid) : new Guid(b.Buffer);
        }


    }
}
