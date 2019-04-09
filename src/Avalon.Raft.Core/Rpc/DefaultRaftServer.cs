using Avalon.Raft.Core.Persistence;
using Avalon.Raft.Core.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Polly;

namespace Avalon.Raft.Core.Rpc
{
    public class DefaultRaftServer : IRaftServer, IDisposable
    {
        static class Queues
        {
            public const string PeerAppendLog = "Peer-AppendLog-";
            public const string PeerFactory = "PeerFactory-";
            public const string LogCommit = "LogCommit";
            public const string HeartBeatReceive = "HeartBeatReceive";
            public const string HeartBeatSend = "HeartBeatSend";
            public const string Candidacy = "Candidacy";
            public const string ApplyClientCommands = "ApplyClientCommands";
        }

        protected VolatileState _volatileState = new VolatileState();
        protected VolatileLeaderState _volatileLeaderState = new VolatileLeaderState();
        protected Role _role;
        protected readonly object _lock = new object();
        protected DateTimeOffsetTimestamp _lastHeartbeat = new DateTimeOffsetTimestamp();
        protected DateTimeOffsetTimestamp _lastHeartbeatSent = new DateTimeOffsetTimestamp();

        protected readonly IStateMachine _stateMachine;
        protected readonly ILogPersister _logPersister;
        protected readonly IPeerManager _peerManager;
        protected readonly RaftSettings _settings;
        protected readonly RaftServerSettings _serverSettings;
        protected int _candidateVotes;
        protected WorkerPool _workers;
        protected readonly AutoPersistentState _state;
        protected string _leaderAddress;
        public event EventHandler<RoleChangedEventArgs> RoleChanged;

        public Role Role => _role;
        public PersistentState State => _state;

        public DateTimeOffsetTimestamp LastHeartBeat
        {
            get
            {
                return _lastHeartbeat;
            }
            internal set
            {
                _lastHeartbeat = value;
            }
        }

        public DateTimeOffsetTimestamp LastHeartBeatSent
        {
            get
            {
                return _lastHeartbeatSent;
            }
            internal set
            {
                _lastHeartbeatSent = value;
            }
        }

        #region .ctore and setup

        public DefaultRaftServer(
            ILogPersister logPersister, 
            IStatePersister statePersister, 
            IStateMachine stateMachine,
            IPeerManager peerManager, 
            RaftSettings settings,
            RaftServerSettings serverSettings = null)
        {
            _logPersister = logPersister;
            _peerManager = peerManager;
            _stateMachine = stateMachine;
            _settings = settings;
            _state = new AutoPersistentState(statePersister);
            _serverSettings = serverSettings ?? new RaftServerSettings();
            SetupPool();
        }

        private void SetupPool()
        {
            var names = new List<string>();
            foreach (var p in _peerManager.GetPeers())
            {
                names.Add(Queues.PeerAppendLog + p.Address);
            }

            names.Add(Queues.LogCommit);
            names.Add(Queues.Candidacy);
            names.Add(Queues.HeartBeatReceive);
            names.Add(Queues.HeartBeatSend);

            _workers = new WorkerPool(names.ToArray());
            _workers.Start();

            // LogCommit
            Func<Task> logCommit = LogCommit;
            _workers.Enqueue(Queues.LogCommit, 
                new Job(logCommit.ComposeLooper(_settings.ElectionTimeoutMin.Multiply(0.2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

            // candidacy
            Func<Task> candidacy = Candidacy;
            _workers.Enqueue(Queues.Candidacy,
                new Job(candidacy.ComposeLooper(_settings.CandidacyTimeout.Multiply(0.2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

            // receiving heartbeat
            Func<Task> hbr = HeartBeatReceive;
            _workers.Enqueue(Queues.HeartBeatReceive,
                new Job(hbr.ComposeLooper(_settings.ElectionTimeoutMin.Multiply(0.2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

            // sending heartbeat
            Func<Task> hbs = HeartBeatSend;
            _workers.Enqueue(Queues.HeartBeatSend,
                new Job(hbs.ComposeLooper(_settings.ElectionTimeoutMin.Multiply(0.2)),
                TheTrace.LogPolicy().RetryForeverAsync()));

            TheTrace.TraceInformation("Setup finished.");
        }

        #endregion

        #region Work Streams

        private Task HeartBeatReceive()
        {
            var millis = new Random().Next((int)_settings.ElectionTimeoutMin.TotalMilliseconds, (int)_settings.ElectionTimeoutMax.TotalMilliseconds + 1);
            var elapsed = _lastHeartbeat.Since().TotalMilliseconds;
            if (_role == Role.Follower && elapsed > millis)
            {
                TheTrace.TraceInformation("Timeout for heartbeat: {0}ms. Time for candidacy!", elapsed);
                BecomeCandidate();
            }

            return Task.CompletedTask;
        }

        private async Task HeartBeatSend()
        {
            if (_role != Role.Leader)
                return;

            if (_lastHeartbeatSent.Since() < _settings.ElectionTimeoutMin.Multiply(0.2))
                return;

            var currentTerm = State.CurrentTerm; // create a var. Could change during the method leading to confusing logs.

            var req = new AppendEntriesRequest()
            {
                CurrentTerm = currentTerm,
                Entries = new byte[0][],
                LeaderCommitIndex = _volatileState.CommitIndex,
                LeaderId = State.Id,
                PreviousLogIndex = long.MaxValue,
                PreviousLogTerm = long.MaxValue
            };

            var peers = _peerManager.GetPeers().ToArray();
            var proxies = peers.Select(x => _peerManager.GetProxy(x.Address));
            var retry = TheTrace.LogPolicy().RetryForeverAsync();
            var policy = Policy.TimeoutAsync(_settings.ElectionTimeoutMin.Multiply(0.2)).WrapAsync(retry);
            var all = await Task.WhenAll(proxies.Select(p => policy.ExecuteAndCaptureAsync(() => p.AppendEntriesAsync(req))));
            var maxTerm = currentTerm;
            foreach (var r in all)
            {
                if (r.Outcome == OutcomeType.Successful)
                {
                    if (!r.Result.IsSuccess)
                        TheTrace.TraceWarning("Got this from a client: {0}", r.Result.Reason);

                    // NOTE: We do NOT change leadership if they send higher term, since they could be candidates whom will not become leaders
                    // we actually do not need to do anything with the result other than logging it
                    if (r.Result.CurrentTerm > maxTerm)
                        maxTerm = r.Result.CurrentTerm;
                }
            }

            if (maxTerm > State.CurrentTerm)
                TheTrace.TraceWarning("Revolution brewing. Terms as high as {} (vs my {}) were seen.", maxTerm, currentTerm);

            _lastHeartbeatSent.Set();
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
                        if (r.Result.CurrentTerm > maxTerm)
                            maxTerm = r.Result.CurrentTerm;

                        if (r.Result != null && r.Result.VoteGranted)
                            forMe++;
                        else
                            againstMe++;
                    }
                }

                if (againstMe >= concensus)
                {
                    BecomeFollower(maxTerm);
                    TheTrace.TraceInformation("Result of the candidacy for term {0}. I got rejected with {1} votes :/", State.CurrentTerm, againstMe);
                }
                else if (forMe >= concensus)
                {
                    BecomeLeader();
                    TheTrace.TraceInformation("Result of the candidacy for term {0}. I got elected with {1} votes! :)", State.CurrentTerm, forMe);
                }
                else
                {
                    TheTrace.TraceInformation("Result of the candidacy for term {0}. Non-conclusive with {1} for me and {2} against me.", State.CurrentTerm, forMe, againstMe);
                }
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

        private Func<Task> PeerAppendLog(Peer peer)
        {
            return async () =>
            {
                long nextIndex;
                long matchIndex;

                var hasMatch = _volatileLeaderState.MatchIndices.TryGetValue(peer.Id, out matchIndex);
                var hasNext = _volatileLeaderState.NextIndices.TryGetValue(peer.Id, out nextIndex);
                var myLastIndex = _logPersister.LastIndex;

                if (!hasMatch)
                {
                    TheTrace.TraceWarning("Could not find peer with id {} and address {} in matchIndex dic.", peer.Id, peer.Address);
                    return;
                }

                if (!hasNext)
                {
                    TheTrace.TraceWarning("Could not find peer with id {} and address {} in nextIndex dic.", peer.Id, peer.Address);
                    return;
                }

                if (nextIndex > myLastIndex)
                    return; // nothing to do

                var count = (int) Math.Min(_settings.MaxNumberLogEntriesToAskToBeAppended, myLastIndex - nextIndex);
                var proxy = _peerManager.GetProxy(peer.Address);
                var retry = TheTrace.LogPolicy().RetryForeverAsync();
                var policy = Policy.TimeoutAsync(_settings.CandidacyTimeout).WrapAsync(retry);
                var previousIndexTerm = -1L;
                if (nextIndex > 0)
                    previousIndexTerm = _logPersister.GetEntries(nextIndex - 1, 1).First().Term;

                var request = new AppendEntriesRequest()
                {
                    CurrentTerm = State.CurrentTerm,
                    Entries = _logPersister.GetEntries(nextIndex, count).Select(x => x.Body).ToArray(),
                    LeaderCommitIndex = _volatileState.CommitIndex,
                    LeaderId = State.Id,
                    PreviousLogIndex = nextIndex - 1,
                    PreviousLogTerm = previousIndexTerm
                };

                var result = await policy.ExecuteAndCaptureAsync(() => proxy.AppendEntriesAsync(request));
                if (result.Outcome == OutcomeType.Successful)
                {
                    if (result.Result.IsSuccess)
                    {
                        // If successful: update nextIndex and matchIndex for follower(§5.3)"
                        _volatileLeaderState.MatchIndices[peer.Id] = _volatileLeaderState.NextIndices[peer.Id] = nextIndex + count;
                        TheTrace.TraceInformation("Successfully transferred {} entries from index {} to peer {}", count, nextIndex, peer.Id);
                        UpdateCommitIndex();
                    }
                    else
                    {
                        // log reason only
                        TheTrace.TraceWarning("AppendEntries for start index {} and count {} for peer {} with address {} in term {} failed with this reason: ", 
                            nextIndex,
                            count,
                            peer.Id,
                            peer.Address,
                            State.CurrentTerm,
                            result.Result.Reason);

                        if (result.Result.ReasonType == ReasonType.LogInconsistency)
                        {
                            var diff = nextIndex - (matchIndex + 1);
                            nextIndex = diff > _settings.MaxNumberOfDecrementForLogsThatAreBehind ?
                                nextIndex - _settings.MaxNumberOfDecrementForLogsThatAreBehind :
                                nextIndex - diff;

                            _volatileLeaderState.NextIndices[peer.Id] = nextIndex;
                            TheTrace.TraceInformation("Updated (decremented) next index for peer {} to {}", peer.Id, nextIndex);
                        }
                    }
                }
                else
                {
                    // NUNCA!!
                    // not interested in network, etc errors, they get logged in the policy
                }
            };
        }

        internal void UpdateCommitIndex()
        {
            /*
            If there exists an N such that N > commitIndex, a majority
            of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            set commitIndex = N(§5.3, §5.4). 
            */

            var majorityIndex = _volatileLeaderState.GetMajorityMatchIndex();
            var index = _volatileState.CommitIndex + 1; // next
            while(index <= _logPersister.LastIndex && index <= majorityIndex && _logPersister.GetEntries(index, 1).First().Term == State.CurrentTerm)
            {
                index++;
            }

            _volatileState.CommitIndex = index - 1;
        }

        #endregion

        #region RPC

        /// <inheritdoc />
        public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
        {
            _lastHeartbeat.Set();
            string message = null;

            if (request.CurrentTerm > State.CurrentTerm)
                BecomeFollower(request.CurrentTerm);

            // Reply false if term < currentTerm (§5.1)
            if (request.CurrentTerm < State.CurrentTerm)
            {
                message = $"Leader's term is behind ({request.CurrentTerm} vs {State.CurrentTerm}).";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.TermInconsistency, message));
            }

            if (request.PreviousLogIndex > _logPersister.LastIndex)
            {
                message = $"Position for last log entry is {_logPersister.LastIndex} but got entries starting at {request.PreviousLogIndex}";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.LogInconsistency, message));
            }

            if (request.Entries == null || request.Entries.Length == 0) // it is a heartbeat, set the leader address
            {
                _leaderAddress = _peerManager.GetPeers().Where(x => x.Id == request.LeaderId).FirstOrDefault()?.Address;
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
                    return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.LogInconsistency, message));
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
            TheTrace.TraceInformation($"Current last index is {_logPersister.LastIndex}. About to append {entries.Length} entries at {request.PreviousLogIndex + 1}");
            _logPersister.Append(entries, request.PreviousLogIndex + 1);

            //If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.LeaderCommitIndex > _volatileState.CommitIndex)
            {
                _volatileState.CommitIndex = Math.Min(request.LeaderCommitIndex, _logPersister.LastIndex);
            }

            message = $"Appended {request.Entries.Length} entries at position {request.PreviousLogIndex + 1}";
            TheTrace.TraceInformation(message);
            return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, true, ReasonType.None, message));
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
                    CurrentTerm = State.CurrentTerm,
                    VoteGranted = false
                });

            // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote(§5.2, §5.4)
            if (!State.LastVotedForId.HasValue && _logPersister.LastIndex <= request.LastLogIndex)
            {
                State.LastVotedForId = request.CandidateId;

                // If election timeout elapses without receiving AppendEntries RPC from current leader OR GRANTING VOTE TO CANDIDATE: convert to candidate
                _lastHeartbeat.Set();

                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTerm = State.CurrentTerm,
                    VoteGranted = true
                });
            }

            // assume the rest we send back no
            return Task.FromResult(new RequestVoteResponse()
            {
                CurrentTerm = State.CurrentTerm,
                VoteGranted = false
            });
        }

        /// <inheritdoc />
        public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
        {
            if (Role == Role.Leader)
            {
                return Task.FromResult(new StateMachineCommandResponse() {Outcome = CommandOutcome.Accepted });
            }
            else if (_serverSettings.ExecuteStateMachineCommandsOnClientBehalf && _leaderAddress != null)
            {
                var leaderProxy = _peerManager.GetProxy(_leaderAddress);
                return leaderProxy.ApplyCommandAsync(command);
            }
            else if (_serverSettings.RedirectStateMachineCommands && _leaderAddress != null)
            {
                return Task.FromResult(new StateMachineCommandResponse() 
                {
                    Outcome = CommandOutcome.Redirect, 
                    DirectTo = _leaderAddress 
                });
            }
            else
            {
                return Task.FromResult(new StateMachineCommandResponse() {Outcome = CommandOutcome.ServiceUnavailable });
            }
        }


        #endregion

        #region Role Changes

        protected void OnRoleChanged(Role role)
        {
            State.LastVotedForId = null;
            RoleChanged?.Invoke(this, new RoleChangedEventArgs(role));
        }

        private void BecomeFollower(long term)
        {
            DestroyPeerAppendLogJobs();
            _lastHeartbeat.Set(); // important not to become candidate again at least for another timeout
            State.CurrentTerm = term;
            OnRoleChanged(_role = Role.Follower);
        }

        private void BecomeLeader()
        {
            _leaderAddress = null;
            _volatileLeaderState = new VolatileLeaderState();
            var peers = _peerManager.GetPeers().ToArray();
            foreach(var peer in peers)
            {
                _volatileLeaderState.NextIndices[peer.Id] = _logPersister.LastIndex + 1;
                _volatileLeaderState.MatchIndices[peer.Id] = -1L;
            }

            SetupPeerAppendLogJobs(peers);
            OnRoleChanged(_role = Role.Leader);
        }

        private void BecomeCandidate()
        {
            _leaderAddress = null;
            DestroyPeerAppendLogJobs();
            State.IncrementTerm();
            OnRoleChanged(_role = Role.Candidate);
        }

        private void SetupPeerAppendLogJobs(IEnumerable<Peer> peers)
        {
            foreach(var p in peers)
            {
                var todo = PeerAppendLog(p);
                _workers.Enqueue(Queues.PeerAppendLog + p.Address,
                    new Job(todo.ComposeLooper(TimeSpan.Zero), // the timeout is built into the job
                    TheTrace.LogPolicy().WaitAndRetryAsync(3, (i) => TimeSpan.FromMilliseconds(i*i*50))));
            }
        }

        private void DestroyPeerAppendLogJobs()
        {
            foreach(var w in _workers.GetWorkers(Queues.PeerAppendLog))
                w.Stop();
        }

        #endregion

        public void Dispose()
        {
            TheTrace.TraceInformation("Disposing server.");
            _workers.Stop();
            TheTrace.TraceInformation("Disposing server. Workers stopped.");
            _logPersister.Dispose();
            TheTrace.TraceInformation("Disposing server. Log Persister stopped.");
        }
    }
}
