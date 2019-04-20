using System.IO;
using System.Threading.Tasks;

namespace Avalon.Raft.Core
{
    public interface IStateMachine
    {
        Task ApplyAsync(byte[][] commands);

        Task WriteSnapshotAsync(Stream stream);

        Task RebuildFromSnapshotAsync(Snapshot snapshot);
    }
}
