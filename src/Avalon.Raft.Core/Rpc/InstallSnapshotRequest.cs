using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// Install snapshot on followers
    /// </summary>
    public class InstallSnapshotRequest
    {
        /// <summary>
        /// leader’s term
        /// </summary>
        public long CurrentTerm { get; set; }

        /// <summary>
        /// so follower can redirect clients
        /// </summary>
        public Guid LeaderId { get; set; }

        /// <summary>
        /// the snapshot replaces all entries up through and including this index
        /// </summary>
        public long LastIncludedIndex { get; set; }

        /// <summary>
        /// term of lastIncludedIndex
        /// </summary>
        public long LastIncludedTerm { get; set; }

        /// <summary>
        /// byte offset where chunk is positioned in the snapshot file
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// raw bytes of the snapshot chunk, starting at offset
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// true if this is the last chunk
        /// </summary>
        public bool IsDone { get; set; }
    }
}
