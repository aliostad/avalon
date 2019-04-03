using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// An abstraction to provide testability
    /// </summary>
    public class DateTimeOffsetTimestamp
    {
        private DateTimeOffset _timestamp;

        public DateTimeOffsetTimestamp() : this(DateTimeOffset.Now)
        {
        }

        public DateTimeOffsetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
        }

        public void Set()
        {
            Set(DateTimeOffset.Now);
        }

        public void Set(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
        }

        public TimeSpan Since()
        {
            return Since(DateTimeOffset.Now);
        }

        public virtual TimeSpan Since(DateTimeOffset time)
        {
            return time.Subtract(_timestamp);
        }

    }
}
