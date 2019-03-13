using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    /// <summary>
    /// Manages the Raft Log file. 
    /// 
    /// NOTE:
    /// This interface is intentially non-async since it is meant to used by a dedicated thread
    /// </summary>
    public interface ILogPersister
    {
        /// <summary>
        /// Append entries
        /// </summary>
        /// <param name="entries">entries</param>
        /// <param name="startingOffset">first entry has this index</param>
        void Append(LogEntry[] entries, long startingOffset);

        /// <summary>
        /// Get entries from the position
        /// </summary>
        /// <param name="index"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        LogEntry[] GetEntries(long index, int count);
       
        /// <summary>
        /// Delete entries from this index to the end of log
        /// </summary>
        /// <param name="fromIndex">From this index (and including)</param>
        void DeleteEntries(long fromIndex);

        /// <summary>
        /// Write a snapshot chunk
        /// </summary>
        /// <param name="lastIncludedIndex">Lats included index in the whole snapshot</param>
        /// <param name="chunk">chunk of the snapshot to be written</param>
        /// <param name="offsetInFile">position of the data in the snapshot file</param>
        /// <param name="isFinal">whether this is the last chunk</param>
        void WriteSnapshot(long lastIncludedIndex, byte[] chunk, long offsetInFile, bool isFinal);

        /// <summary>
        /// In case of snapshotting, what is the offset of the log starting from
        /// </summary>
        long LogOffset { get; }

        /// <summary>
        /// Last index of the log
        /// </summary>
        long LastIndex { get; }

        /// <summary>
        /// Term of the last entry
        /// </summary>
        long LastEntryTerm { get; }
    }
}
