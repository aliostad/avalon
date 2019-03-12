using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Rpc
{
    public interface IRaftServer
    {
        Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request);

        Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request);

        Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request);

        Role Role { get; }

        event EventHandler<RoleChangedEventArgs> RoleChanged;
    }

    public class RoleChangedEventArgs : EventArgs
    {
        public RoleChangedEventArgs(Role newRole)
        {
            NewRole = newRole;
        }

        public Role NewRole { get; }
    }
}
