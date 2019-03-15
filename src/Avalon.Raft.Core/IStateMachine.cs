using System.IO;
using System.Threading.Tasks;

namespace Avalon.Raft.Core
{
    public interface IStateMachine
    {
        Task ApplyAsync(LogEntry[] entries);

        Task WriteSnapshotAsync(Stream stream);

        Task RebuildFromSnapshotAsync(Snapshot snapshot);
    }
}
