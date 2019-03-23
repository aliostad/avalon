using Avalon.Raft.Core.Persistence;
using Avalon.Raft.Core.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using System.Threading;

namespace Avalon.Raft.Core.Rpc
{
    public class DefaultRaftServer : IRaftServer, IDisposable
    {
        class Queues
        {
            public const string PeerAppendLog = "Peer-AppendLog-";
            public const string PeerVote = "Peer-Vote-";
            public const string PeerFactory = "PeerFactory-";
            public const string LogCommit = "LogCommit";
            public const string HeartBeatReceive = "HeartBeatReceive";
            public const string Candidacy = "Candidacy";
        }

        protected VolatileState _volatileState = new VolatileState();
        protected Role _role;
        protected readonly object _lock = new object();
        protected DateTimeOffset _lastHeartbeat = DateTimeOffset.Now;

        protected readonly IStateMachine _stateMachine;
        protected readonly ILogPersister _logPersister;
        protected readonly IPeerManager _peerManager;
        protected readonly RaftSettings _settings;
        protected int _candidateVotes;
        protected WorkerPool _workers;
        protected readonly AutoPersistentState _state;

        public Role Role => _role;
        public PersistentState State => _state;

        public event EventHandler<RoleChangedEventArgs> RoleChanged;

        #region .ctore and setup

        public DefaultRaftServer(ILogPersister logPersister, IStatePersister statePersister, IStateMachine stateMachine,
            IPeerManager peerManager, RaftSettings settings)
        {
            _logPersister = logPersister;
            _peerManager = peerManager;
            _stateMachine = stateMachine;
            _settings = settings;
            _state = new AutoPersistentState(statePersister);
            SetupPool();
        }

        private void SetupPool()
        {
            var names = new List<string>();
            foreach (var p in _peerManager.GetPeers())
            {
                names.Add(Queues.PeerAppendLog + p.Address);
                names.Add(Queues.PeerVote + p.Address);
            }

            names.Add(Queues.LogCommit);
            names.Add(Queues.Candidacy);
            names.Add(Queues.HeartBeatReceive);

            _workers = new WorkerPool(names.ToArray());
            _workers.Start();

            // LogCommit
            Func<Task> logCommit = LogCommit;
            _workers.Enqueue(Queues.LogCommit, 
                new Job(logCommit.ComposeLooper(_settings.ElectionTimeoutMin.Multiply(1/2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

            Func<Task> candidacy = Candidacy;
            _workers.Enqueue(Queues.Candidacy,
                new Job(candidacy.ComposeLooper(_settings.CandidacyTimeout),
                TheTrace.LogPolicy().RetryForeverAsync()));

            Func<Task> hbr = HeartBeatReceive;
            _workers.Enqueue(Queues.HeartBeatReceive,
                new Job(hbr.ComposeLooper(_settings.ElectionTimeoutMin.Multiply(1/2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

        }

        #endregion

        #region Work Streams

        private Task HeartBeatReceive()
        {
            var millis = new Random().Next((int)_settings.ElectionTimeoutMin.TotalMilliseconds, (int)_settings.ElectionTimeoutMax.TotalMilliseconds);
            if (_role == Role.Follower && DateTimeOffset.Now.Subtract(_lastHeartbeat).TotalMilliseconds > millis)
            {
                BecomeCandidate();
            }

            return Task.CompletedTask;
        }

        private async Task Candidacy()
        {
            var forMe = 1; // vote for yourself
            var againstMe = 0;

            while (_role == Role.Candidate)
            {
                var peers = _peerManager.GetPeers().ToArray();
                var concensus = (peers.Length / 2) + 1;
                var proxies = peers.Select(x => _peerManager.GetProxy(x.Address));
                var retry = TheTrace.LogPolicy().RetryForeverAsync();
                var policy = Policy.TimeoutAsync(_settings.CandidacyTimeout).WrapAsync(retry);
                var request = new RequestVoteRequest()
                {
                    CandidateId = State.Id,
                    CurrentTerm = State.CurrentTerm,
                    LastLogIndex = _logPersister.LastIndex,
                    LastLogTerm = _logPersister.LastEntryTerm
                };

                var all = await Task.WhenAll(proxies.Select(p => policy.ExecuteAndCaptureAsync(() => p.RequestVoteAsync(request))));
                var maxTerm = 0L;
                foreach (var r in all)
                {
                    if (r.Outcome == OutcomeType.Successful)
                    {
                        if (r.Result.CurrentTrem > maxTerm)
                            maxTerm = r.Result.CurrentTrem;

                        if (r.Result != null && r.Result.VoteGranted)
                            forMe++;
                        else
                            againstMe++;
                    }
                }

                if (againstMe >= concensus)
                    BecomeFollower(maxTerm);
                else if (forMe >= concensus)
                    BecomeLeader();
            }
        }

        private async Task LogCommit()
        {
            // IMPORTANTE!! SIEMPERE CREATE VARIABLES LOCALES
            var commitIndex = _volatileState.CommitIndex;
            var lastApplied = _volatileState.LastApplied;

            if (commitIndex > lastApplied && _workers.IsEmpty(Queues.LogCommit)) // check ONLY if empty. NO RE-ENTRY
            {
                // ADD BATCHING LATER
                await _stateMachine.ApplyAsync(
                    _logPersister.GetEntries(lastApplied + 1, (int)(commitIndex - lastApplied)));

                _volatileState.LastApplied = commitIndex;
            }
        }

        #endregion

        #region RPC

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


        #endregion

        #region Role Chnages

        protected void OnRoleChanged(Role role)
        {
            RoleChanged?.Invoke(this, new RoleChangedEventArgs(role));
        }

        private void BecomeFollower(long term)
        {
            OnRoleChanged(_role = Role.Follower);
        }

        private void BecomeLeader()
        {
            OnRoleChanged(_role = Role.Leader);
        }

        private void BecomeCandidate()
        {
            OnRoleChanged(_role = Role.Candidate);
        }

        #endregion

        public void Dispose()
        {
            _workers.Stop();
        }
    }
}
