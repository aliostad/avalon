using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    public class RaftSettings
    {
        /// <summary>
        /// Minimum time when a follower waits for heartbeat before decides to become a candidate. Suggested 100ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMin { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Maximum time when a follower waits for heartbeat before decides to become a candidate. Suggested 500ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMax { get; set; } = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// How long candidate waits for RequestVote. Probably 50-100ms
        /// </summary>
        public TimeSpan CandidacyTimeout { get; set; } = TimeSpan.FromMilliseconds(150);

        /// <summary>
        /// Minimum index interval between snapshots
        /// </summary>
        public long MinSnapshottingIndexInterval { get; set; } = 100_000L;
    }
}
