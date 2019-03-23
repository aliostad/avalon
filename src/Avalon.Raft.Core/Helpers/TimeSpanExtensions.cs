using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    public static class TimeSpanExtensions
    {
        public static TimeSpan Multiply(this TimeSpan t, double value)
        {
            return new TimeSpan((long) (t.Ticks * value));
        }
    }
}
