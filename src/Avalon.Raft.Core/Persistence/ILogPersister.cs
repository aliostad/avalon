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
    public interface ILogPersister : IDisposable
    {
        /// <summary>
        /// Append entries
        /// </summary>
        /// <param name="entries">entries</param>
        /// <param name="startingOffset">first entry has this index</param>
        void Append(LogEntry[] entries, long? startingOffset = null);

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
        /// Deletes entries up to but not including an index. Useful during snapshotting
        /// </summary>
        /// <param name="index">Up to but NOT including this index</param>
        void ApplySnapshot(long newFirstIndex);

        /// <summary>
        /// In case of snapshotting, this is what the offset of the log starting from
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
