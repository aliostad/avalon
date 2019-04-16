using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;

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
        public long CommitIndex { get; set; } = -1;

        /// <summary>
        /// index of highest log entry applied to state machine
        /// </summary>
        public long LastApplied { get; set; } = -1;
    }

    /// <summary>
    /// Volatile state on leaders (Reinitialized after election)
    /// </summary>
    public class VolatileLeaderState
    {
        /// <summary>
        /// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) 
        /// </summary>
        private ConcurrentDictionary<Guid, long> NextIndices { get; } = new ConcurrentDictionary<Guid, long>();

        /// <summary>
        /// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        /// </summary>
        private ConcurrentDictionary<Guid, long> MatchIndices { get; } = new ConcurrentDictionary<Guid, long>();

        public bool TryGetNextIndex(Guid id, out long value)
        {
            return NextIndices.TryGetValue(id, out value);
        }

        public bool TryGetMatchIndex(Guid id, out long value)
        {
            return MatchIndices.TryGetValue(id, out value);
        }

        public void SetNextIndex(Guid id, long value)
        {
            TheTrace.TraceInformation($"Setting next index for id {id} to {value} in");
            NextIndices[id] = value;
        }

        public void SetMatchIndex(Guid id, long value)
        {
            TheTrace.TraceInformation($"Setting match index for id {id} to {value}");
            MatchIndices[id] = value;
        }

        /// <summary>
        /// Returns highest match index which is supported by the majority (for §5.3, §5.4) 
        /// </summary>
        public long GetMajorityMatchIndex()
        {
            if (MatchIndices.Count == 0)
                return -1;

            var matchIndices = MatchIndices.Values.ToArray();
            Array.Sort(matchIndices);
            var middle = (matchIndices.Length) / 2; 
            return matchIndices[middle - 1]; // if there are 4 follower nodes, pick the second item
        }
    }
}
