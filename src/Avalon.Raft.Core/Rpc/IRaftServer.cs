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

        Task<InstallUpdateResponse> InstallUpdateAsync(InstallUpdateRequest request);
    }
}
