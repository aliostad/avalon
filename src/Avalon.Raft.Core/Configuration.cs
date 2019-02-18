using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    public class Configuration
    {
        /// <summary>
        /// Minimum time when a follower waits for heartbeat before decides to become a candidate. Suggested 100ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMin { get; set; }

        /// <summary>
        /// Maximum time when a follower waits for heartbeat before decides to become a candidate. Suggested 500ms.
        /// </summary>
        public TimeSpan ElectionTimeoutMax { get; set; }

        /// <summary>
        /// How long candidate waits for RequestVote. Probably 50-100ms
        /// </summary>
        public TimeSpan CandidacyTimeout { get; set; }
    }
}
