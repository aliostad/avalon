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

        /// <summary>
        /// Term to which it was stored
        /// </summary>
        public long Term;

        /*
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

        public static implicit operator LogEntry(Bufferable buffer)
        {
            var entry = new LogEntry() { Body = buffer.Buffer };
            return entry;
        }
        */
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

        /// <summary>
        /// Log index of the entry
        /// </summary>
        public long Index;

        public long Term;

        public static implicit operator byte[] (StoredLogEntry entry)
        {
            Bufferable b = new Bufferable(entry.Body);
            return b.PrefixWithIndexAndTerm(entry.Index, entry.Term).Buffer;
        }

        public static implicit operator StoredLogEntry(byte[] buffer)
        {

            var entry = new StoredLogEntry()
            {
                Index = BitConverter.ToInt64(buffer, 0),
                Term = BitConverter.ToInt64(buffer, sizeof(long)),
                Body = buffer.Splice(sizeof(long) * 2)
            };

            return entry;
        }
    }

}
