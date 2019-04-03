using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Tests
{
    public class AlwaysRecentTimestamp : DateTimeOffsetTimestamp
    {
        public override TimeSpan Since(DateTimeOffset time)
        {
            return TimeSpan.Zero;
        }
    }
}
