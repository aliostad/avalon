﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Avalon.Common
{
    public struct Bufferable
    {
        private readonly byte[] _buffer; 

        public Bufferable(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");

            _buffer = buffer;
        }

        public unsafe Bufferable(params Bufferable[] bufferables)
        {
            if (bufferables == null || bufferables.Length == 0)
                throw new InvalidOperationException("At least must pass one bufferable");

            var lenNeeded = bufferables.Sum(x => x.Buffer.Length);
            _buffer = new byte[lenNeeded];
            var position = 0;
            fixed (byte* destPtr = &_buffer[0])
            {
                foreach (var b in bufferables)
                {
                    fixed (byte* ptr = &b.Buffer[0])
                    {
                        System.Buffer.MemoryCopy(ptr, destPtr + position, b.Buffer.Length, b.Buffer.Length);
                    }

                    position += b.Buffer.Length;
                }
            }
        }

        /// <summary>
        /// Byte array that this object represents
        /// </summary>
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

        public static implicit operator Bufferable(byte[] buffer)
        {
            return new Bufferable(buffer);
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

    public static class BufferableExtensions
    {
        public static unsafe Bufferable PrefixWithIndexAndTerm(this Bufferable b, long index, long term)
        {
            var buffer = new byte[sizeof(long)*2 + b.Buffer.Length];
            fixed (byte* ptr = &b.Buffer[0], destPtr = &buffer[0])
            {
                Buffer.MemoryCopy(Unsafe.AsPointer(ref index), destPtr, buffer.Length, sizeof(long));
                Buffer.MemoryCopy(Unsafe.AsPointer(ref term), destPtr + sizeof(long), buffer.Length, sizeof(long));
                Buffer.MemoryCopy(ptr, destPtr + sizeof(long)*2, b.Buffer.Length, b.Buffer.Length);
            }

            return new Bufferable(buffer);
        }
    }
}
