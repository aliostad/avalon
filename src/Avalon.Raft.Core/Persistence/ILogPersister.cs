using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public interface ILogPersister
    {
        Task AppendAsync(LogEntry entry);

        Task<LogEntry[]> GetEntriesAsync(int index, int count);
       
        Task DeleteEntriesAsync(int fromIndex);
       
        Task CompactAsync(int fromIndex);

        /// <summary>
        /// In case of snapshotting
        /// </summary>
        int FirstIndexOffset { get; }
    }
}
