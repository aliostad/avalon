using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Rpc
{
    public class DefaultRaftServer : IRaftServer
    {
        protected PersistentState _persistentState = new PersistentState();
        protected VolatileState _volatileState = new VolatileState();

        public async Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
        {
            throw new NotImplementedException();
        }

        public Task<InstallUpdateResponse> InstallUpdateAsync(InstallUpdateRequest request)
        {
            throw new NotImplementedException();
        }

        public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
        {
            if (_persistentState.CurrentTerm > request.CurrentTerm)
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTrem = _persistentState.CurrentTerm,
                    VoteGranted = false
                });

            if (!_persistentState.LastVotedForId.HasValue || _volatileState.CommitIndex <= request.LastLogIndex)
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTrem = _persistentState.CurrentTerm,
                    VoteGranted = true
                });

            // assume the rest we send back no
            return Task.FromResult(new RequestVoteResponse()
            {
                CurrentTrem = _persistentState.CurrentTerm,
                VoteGranted = false
            });

        }
    }
}
