using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Common
{
    public static class UnsafeExtensions
    {

        /// <summary>
        /// Creates a new buffer and copies the length from the index using fast memory copy
        /// </summary>
        /// <param name="buffer">byte buffer</param>
        /// <param name="index">from index</param>
        /// <param name="length">Length to Copy. -1 means all remaining</param>
        /// <returns></returns>
        public static unsafe byte[] Splice(this byte[] buffer, int index, int length = -1)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");

            if (index >= buffer.Length)
                throw new InvalidOperationException($"index of {index} is equal or bigger than the length ({buffer.Length})");

            if (length < 0)
                length = buffer.Length - index;

            if (index + length > buffer.Length)
                throw new InvalidOperationException($"Length requested of {length} from index {index} is bigger than the buffer ({buffer.Length})");

            var newb = new byte[length];
            fixed (byte* srcptr = &buffer[index], destptr = &newb[0])
            {
                Buffer.MemoryCopy(srcptr, destptr, length, length);
            }

            return newb;
        }

    }
}
