using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// Simple class to be inherited for special use cases that use Raft
    /// </summary>
    public class LogEntry
    {
        public byte[] Body { get; set; }

        public static implicit operator byte[] (LogEntry entry)
        {
            return entry.Body;
        }

        public static implicit operator LogEntry (byte[] buffer)
        {
            var entry = new LogEntry() { Body = buffer};
            entry.LoadFromBuffer();
            return entry;
        }

        protected virtual void LoadFromBuffer()
        {
            // nothing. This is from base classes
        }
    }
}
