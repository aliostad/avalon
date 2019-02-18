using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// Volatile state on all servers
    /// </summary>
    public class VolatileState
    {
        /// <summary>
        /// index of highest log entry known to be committed(initialized to 0, increases monotonically)
        /// </summary>
        public long CommitIndex { get; set; }

        /// <summary>
        /// index of highest log entry applied to state machine
        /// </summary>
        public long LastApplied { get; set; }
    }

    /// <summary>
    /// Volatile state on leaders (Reinitialized after election)
    /// </summary>
    public class VolatileLeaderState : VolatileState
    {
        /// <summary>
        /// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) 
        /// </summary>
        public Dictionary<Guid, long> NextIndex { get; } = new Dictionary<Guid, long>();

        /// <summary>
        /// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        /// </summary>
        public Dictionary<Guid, long> MatchIndex { get; } = new Dictionary<Guid, long>();
    }
}
