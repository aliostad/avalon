using System.IO;
using System.Threading.Tasks;

namespace Avalon.Raft.Core
{
    public interface IStateMachine
    {
        Task ApplyAsync(LogEntry entry);

        Task WriteSnapshotAsync(Stream stream);
    }
}
