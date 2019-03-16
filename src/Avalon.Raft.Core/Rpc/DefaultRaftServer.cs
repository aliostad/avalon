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
        protected readonly object _lock = new object();
        protected DateTimeOffset _lastHeartbeat = DateTimeOffset.Now;

        protected readonly IStateMachine _stateMachine;
        protected readonly ILogPersister _logPersister;
        protected readonly IStatePersister _statePersister;

        public Role Role => _role;

        public event EventHandler<RoleChangedEventArgs> RoleChanged;

        public PersistentState State => _statePersister.GetLatest();

        public DefaultRaftServer(ILogPersister logPersister, IStatePersister statePersister, IStateMachine stateMachine)
        {
            _logPersister = logPersister;
        }

        /// <inheritdoc />
        public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
        {
            _lastHeartbeat = DateTimeOffset.Now;
            string message = null;

            if (request.CurrentTerm > State.CurrentTerm)
                BecomeFollower(request.CurrentTerm);


            // Reply false if term < currentTerm (§5.1)
            if (request.CurrentTerm < State.CurrentTerm)
            {
                message = $"Leader's term is behind ({request.CurrentTerm} vs {State.CurrentTerm}).";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, message));
            }

            if (request.PreviousLogIndex > _logPersister.LastIndex)
            {
                message = $"Position for last log entry is {_logPersister.LastIndex} but got entries starting at {request.PreviousLogIndex}";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, message));
            }

            if (request.Entries == null || request.Entries.Length == 0)
            {
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, true));
            }

            if (request.PreviousLogIndex < _logPersister.LastIndex)
            {
                // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
                var entry = _logPersister.GetEntries(request.PreviousLogIndex, 1).First();
                if (entry.Term != request.CurrentTerm)
                {
                    message = $"Position at {request.PreviousLogIndex} has term {entry.Term} but according to leader {request.LeaderId} it must be {request.PreviousLogTerm}";
                    TheTrace.TraceWarning(message);
                    return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, message));
                }

                // If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it(§5.3)
                _logPersister.DeleteEntries(request.PreviousLogIndex + 1);
                TheTrace.TraceWarning("Stripping the log from index {0}. Last index was {1}", request.PreviousLogIndex + 1, _logPersister.LastIndex);
            }
            
            var entries = request.Entries.Select(x => new LogEntry()
            {
                Body = x,
                Term = request.CurrentTerm
            }).ToArray();

            // Append any new entries not already in the log
            _logPersister.Append(entries, request.PreviousLogIndex + 1);

            //If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.LeaderCommitIndex > _volatileState.CommitIndex)
            {
                _volatileState.CommitIndex = Math.Min(request.LeaderCommitIndex, _logPersister.LastIndex);
            }

            message = $"Appended {request.Entries.Length} entries at position {request.PreviousLogIndex + 1}";
            TheTrace.TraceInformation(message);
            return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, true, message));
        }

        /// <inheritdoc />
        public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
        {
            if (request.CurrentTerm > State.CurrentTerm)
                BecomeFollower(request.CurrentTerm);

            _logPersister.WriteSnapshot(request.LastIncludedIndex, request.Data, request.Offset, request.IsDone);
            if (request.IsDone)
            {
                // TODO: rebuild state from snapshot
            }

            return Task.FromResult(new InstallSnapshotResponse() { CurrentTerm = State.CurrentTerm });
        }

        private void BecomeFollower(long term)
        {

        }

        /// <inheritdoc />
        public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
        {
            if (request.CurrentTerm > State.CurrentTerm)
                BecomeFollower(request.CurrentTerm);

            // Reply false if term < currentTerm
            if (State.CurrentTerm > request.CurrentTerm)
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTrem = State.CurrentTerm,
                    VoteGranted = false
                });

            // If votedFor is null or candidateId, and candidate’s log is at least as up - to - date as receiver’s log, grant vote(§5.2, §5.4)
            if (!State.LastVotedForId.HasValue && _logPersister.LastIndex <= request.LastLogIndex)
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
