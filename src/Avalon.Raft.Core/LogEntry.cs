using System;
using System.Collections.Generic;
using System.Text;
using Avalon.Common;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// Simple class to be inherited for special use cases that use Raft
    /// </summary>
    public struct LogEntry
    {
        /// <summary>
        /// Body as buffer representation
        /// </summary>
        public byte[] Body;

        public static implicit operator byte[] (LogEntry entry)
        {
            return entry.Body;
        }

        public static implicit operator LogEntry (byte[] buffer)
        {
            var entry = new LogEntry() { Body = buffer};
            return entry;
        }

        public static implicit operator Bufferable(LogEntry entry)
        {
            return new Bufferable(entry.Body);
        }
    }

    /// <summary>
    /// When we store LogEntry, we store Index along with it
    /// </summary>
    public struct StoredLogEntry
    {
        /// <summary>
        /// Body as buffer representation
        /// </summary>
        public byte[] Body;

        public long Index;

        public static implicit operator byte[] (StoredLogEntry entry)
        {
            Bufferable b = new Bufferable(entry.Body);
            return b.PrefixWithIndex(entry.Index).Buffer;
        }

        public static implicit operator StoredLogEntry(byte[] buffer)
        {

            var entry = new StoredLogEntry()
            {
                Index = BitConverter.ToInt64(buffer, 0),
                Body = buffer.Splice(sizeof(long))
            };

            return entry;
        }
    }

}
