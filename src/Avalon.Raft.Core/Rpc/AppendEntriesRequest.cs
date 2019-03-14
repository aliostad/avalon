using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// 
    /// </summary>
    public class AppendEntriesRequest
    {
        /// <summary>
        /// Term of leader
        /// </summary>
        public long Term { get; set; }

        /// <summary>
        /// so follower can redirect clients
        /// </summary>
        public Guid LeaderId { get; set; }

        /// <summary>
        /// index of log entry immediately preceding new ones
        /// </summary>
        public long PreviousLogIndex { get; set; }

        /// <summary>
        /// term of prevLogIndex entry
        /// </summary>
        public long PreviousLogTerm { get; set; }

        /// <summary>
        /// leader’s commitIndex
        /// </summary>
        public long LeaderCommitIndex { get; set; }

        /// <summary>
        /// Entries. Could be empty.
        /// </summary>
        public byte[][] Entries { get; set; }
    }
}
