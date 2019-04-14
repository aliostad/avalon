using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    public class RaftSettings
    {
        /// <summary>
        /// Minimum time when a follower waits for heartbeat before decides to become a candidate. Default is 100ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMin { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Maximum time when a follower waits for heartbeat before decides to become a candidate. Default is 500ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMax { get; set; } = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// How long candidate waits for RequestVote. Probably 50-150ms
        /// Default is 150ms
        /// </summary>
        public TimeSpan CandidacyTimeout { get; set; } = TimeSpan.FromMilliseconds(150);

        /// <summary>
        /// Minimum index interval between snapshots. Default is 100K.
        /// </summary>
        public long MinSnapshottingIndexInterval { get; set; } = 100_000L;

        /// <summary>
        /// (Not in the paper) Maximum number by which NextIndex for the peer is decremented in case non-match
        /// Default is 100
        /// </summary>
        public long MaxNumberOfDecrementForLogsThatAreBehind { get; set; } = 100;

        /// <summary>
        /// (Not in the paper) Maximum number by of log entries a leader could ask peers to append
        /// Default is 100
        /// </summary>
        public long MaxNumberLogEntriesToAskToBeAppended { get; set; } = 100;

    }
}
