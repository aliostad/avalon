using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public interface ILogPersister
    {
        Task AppendAsync(LogEntry entry);

        Task<LogEntry[]> GetEntriesAsync(long index, int count);
       
        Task DeleteEntriesAsync(long fromIndex);
       
        Task CompactAsync(long fromIndex);

        /// <summary>
        /// In case of snapshotting
        /// </summary>
        long LogOffset { get; }

        long LastIndex { get; }
    }
}
