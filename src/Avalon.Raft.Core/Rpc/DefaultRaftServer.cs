using Avalon.Raft.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Rpc
{
    public class DefaultRaftServer : IRaftServer
    {
        protected VolatileState _volatileState = new VolatileState();
        protected Role _role;
        protected readonly LmdbPersister _persister;
        protected readonly object _lock = new object();

        public Role Role => _role;

        public event EventHandler<RoleChangedEventArgs> RoleChanged;

        public PersistentState State => _persister.GetLatest();

        public DefaultRaftServer(string directory)
        {
            _persister = new LmdbPersister(directory);
        }

        /// <inheritdoc />
        public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
        {
            string message = null;
            if (request.CurrentTerm < State.CurrentTerm)
            {
                message = $"Leader's term is behind ({request.CurrentTerm} vs {State.CurrentTerm}).";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, message));
            }

            if (request.PreviousLogIndex > _persister.LastIndex)
            {
                message = $"Position for last log entry is {_persister.LastIndex} but got entries starting at {request.PreviousLogIndex}";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, message));
            }

            if (request.PreviousLogIndex < _persister.LastIndex)
            {
                _persister.DeleteEntries(request.PreviousLogIndex + 1);
                TheTrace.TraceWarning("Stripping the log from index {0}. Last index was {1}", request.PreviousLogIndex + 1, _persister.LastIndex);
            }

            if (request.Entries != null && request.Entries.Length > 0)
            {
                var entries = request.Entries.Select(x => new LogEntry()
                {
                    Body = x,
                    Term = request.CurrentTerm
                }).ToArray();

                _persister.Append(entries, request.PreviousLogIndex + 1);
            }

            //_persister.Append()
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
        {
            if (State.CurrentTerm > request.CurrentTerm)
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTrem = State.CurrentTerm,
                    VoteGranted = false
                });

            if (!State.LastVotedForId.HasValue || _volatileState.CommitIndex <= request.LastLogIndex)
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTrem = State.CurrentTerm,
                    VoteGranted = true
                });

            // assume the rest we send back no
            return Task.FromResult(new RequestVoteResponse()
            {
                CurrentTrem = State.CurrentTerm,
                VoteGranted = false
            });
        }
    }
}
