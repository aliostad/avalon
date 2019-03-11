using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    public interface ISnapshotOperator
    {
        /// <summary>
        ///Removes old snapshots
        /// </summary>
        void CleanSnapshots();

        /// <summary>
        /// Returns latest snapshot
        /// </summary>
        /// <param name="snapshot">The snapshot</param>
        /// <returns>Whether there was a snapshot to return</returns>
        bool TryGetLastSnapshot(out Snapshot snapshot);

        /// <summary>
        /// Prepares a (file) stream for next snapshot
        /// </summary>
        /// <param name="lastIndexIncluded">Index of the last log entry applied</param>
        /// <returns>Returns the snapshot stream to be writteb to</returns>
        Stream GetNextSnapshotStream(long lastIndexIncluded);

        /// <summary>
        /// Stream is now complete with the snapshot and needs to be finalised
        /// </summary>
        /// <param name="lastIndexIncluded">Index of the last log entry applied</param>
        void FinaliseSnapshot(long lastIndexIncluded);
    }
}
