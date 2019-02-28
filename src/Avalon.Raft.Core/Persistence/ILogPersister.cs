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
        void Append(LogEntry[] entries, long startingOffset);

        LogEntry[] GetEntries(long index, int count);
       
        void DeleteEntries(long fromIndex);

        void WriteSnapshot(long lastIncludedIndex, byte[] chunk, long offsetInFile, bool isFinal);

        /// <summary>
        /// In case of snapshotting
        /// </summary>
        long LogOffset { get; }

        long LastIndex { get; }
    }
}
