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
        public byte[] Body { get; set; }

        public static implicit operator byte[] (LogEntry entry)
        {
            return entry.Body;
        }

        public static implicit operator LogEntry (byte[] buffer)
        {
            var entry = new LogEntry() { Body = buffer};
            return entry;
        }
    }

    public static class LogExtryExtensions
    {
        /*
        public static Bufferable ToBufferWithIndex(this LogEntry entry, long index)
        {
            Buffer.Co
        } */
    }
}
