﻿using Avalon.Raft.Core.Persistence;
using Avalon.Raft.Core.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Polly;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Avalon.Raft.Core.Chaos;

namespace Avalon.Raft.Core.Rpc
{
    public class DefaultRaftServer : IRaftServer, IDisposable
    {
        static class Queues
        {
            public const string PeerAppendLog = "Peer-AppendLog-"; // leaders
            public const string LogCommit = "LogCommit";
            public const string HeartBeatReceiveAndCandidacy = "HeartBeatReceiveAndCandidacy";
            public const string HeartBeatSend = "HeartBeatSend"; // leaders
            public const string ProcessCommandQueue = "ProcessCommandQueue"; // leaders
            public const string CreateSnapshot = "CreateSnapshot"; // leaders
        }

        protected VolatileState _volatileState = new VolatileState();
        protected VolatileLeaderState _volatileLeaderState = new VolatileLeaderState();
        protected Role _role;
        protected readonly object _lock = new object();
        protected DateTimeOffsetTimestamp _lastHeartbeat = new DateTimeOffsetTimestamp();
        protected DateTimeOffsetTimestamp _lastHeartbeatSent = new DateTimeOffsetTimestamp();

        protected readonly IStateMachine _stateMachine;
        protected readonly ILogPersister _logPersister;
        protected readonly ISnapshotOperator _snapshotOperator;
        protected readonly IPeerManager _peerManager;
        protected readonly RaftServerSettings _settings;
        protected readonly Peer _meAsAPeer;
        protected readonly IChaos _chaos;
        protected int _candidateVotes;
        protected WorkerPool _workers;
        protected readonly AutoPersistentState _state;
        protected string _leaderAddress;
        private bool _isSnapshotting = false;
        private BlockingCollection<StateMachineCommandRequest> _commands = new BlockingCollection<StateMachineCommandRequest>();
        public event EventHandler<RoleChangedEventArgs> RoleChanged;


        #region For Testability

        internal ISnapshotOperator SnapshotOperator => _snapshotOperator;

        internal ILogPersister LogPersister => _logPersister;

        internal VolatileState VolatileState => _volatileState;

        internal VolatileLeaderState VolatileLeaderState => _volatileLeaderState;

        internal BlockingCollection<StateMachineCommandRequest> Commands => _commands;

        internal int SuccessfulSnapshotCreations {get; set;}
        internal int SuccessfulSnapshotInstallations {get; set;}

        #endregion

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
            ISnapshotOperator snapshotOperator,
            IStateMachine stateMachine,
            IPeerManager peerManager,
            RaftServerSettings settings,
            Peer meAsAPeer = null,
            IChaos chaos = null)
        {
            _logPersister = logPersister;
            _peerManager = peerManager;
            _stateMachine = stateMachine;
            _snapshotOperator = snapshotOperator;
            _settings = settings;
            _state = new AutoPersistentState(statePersister);
            _meAsAPeer = meAsAPeer ?? new Peer("NoAddress", State.Id);
            _chaos = chaos ?? new NoChaos();
        }

        public void Start()
        {
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
            names.Add(Queues.HeartBeatReceiveAndCandidacy);
            names.Add(Queues.HeartBeatSend);
            names.Add(Queues.ProcessCommandQueue);
            names.Add(Queues.CreateSnapshot);

            _workers = new WorkerPool(names.ToArray());
            _workers.Start();

            // LogCommit
            Func<CancellationToken, Task> logCommit = LogCommit;
            _workers.Enqueue(Queues.LogCommit,
                new Job(logCommit,
                TheTrace.LogPolicy(_meAsAPeer.ShortName).RetryForeverAsync(),
                _settings.ElectionTimeoutMin.Multiply(0.2)));

            // receiving heartbeat
            Func<CancellationToken, Task> hbr = HeartBeatReceive;
            _workers.Enqueue(Queues.HeartBeatReceiveAndCandidacy,
                new Job(hbr,
                TheTrace.LogPolicy(_meAsAPeer.ShortName).RetryForeverAsync(),
                _settings.ElectionTimeoutMin.Multiply(0.2)));

            // sending heartbeat
            Func<CancellationToken, Task> hbs = HeartBeatSend;
            _workers.Enqueue(Queues.HeartBeatSend,
                new Job(hbs,
                TheTrace.LogPolicy(_meAsAPeer.ShortName).RetryForeverAsync(),
                _settings.ElectionTimeoutMin.Multiply(0.2)));

            // Applying commands received from the clients
            Func<CancellationToken, Task> cs = CreateSnapshot;
            _workers.Enqueue(Queues.CreateSnapshot,
                new Job(cs,
                TheTrace.LogPolicy(_meAsAPeer.ShortName).WaitAndRetryAsync(2, (i) => TimeSpan.FromMilliseconds(i * i * 50)),
                _settings.ElectionTimeoutMin.Multiply(0.2)));

            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Setup finished.");
        }

        #endregion

        #region Work Streams

        private Task ProcessCommandsQueue(CancellationToken c)
        {
            if (_isSnapshotting)
                return Task.CompletedTask;

            if (Role == Role.Leader)
                TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] ProcessCommandsQueue with {_commands.Count} items.");

            var entries = new List<LogEntry>();
            StateMachineCommandRequest command;

            TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] ProcessCommandsQueue - Before with trying to take.");
            while (_commands.TryTake(out command, 10)) // we can set up a maximum but really we should accept all
            {
                TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] ProcessCommandsQueue - Got an item.");
                entries.Add(new LogEntry()
                {
                    Body = command.Command,
                    Term = State.CurrentTerm
                });
            }

            
            if (entries.Any())
            {
                TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] ProcessCommandsQueue - Before applying {entries.Count} entries.");
                _logPersister.Append(entries.ToArray());
                TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] ProcessCommandsQueue - His Majesty appended {entries.Count} entries.");
            }


            return Task.CompletedTask;
        }

        private Task HeartBeatReceive(CancellationToken c)
        {
            var millis = new Random().Next((int)_settings.ElectionTimeoutMin.TotalMilliseconds, (int)_settings.ElectionTimeoutMax.TotalMilliseconds + 1);
            var elapsed = _lastHeartbeat.Since().TotalMilliseconds;
            if (_role == Role.Follower && elapsed > millis)
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Timeout for heartbeat: {elapsed}ms. Time for candidacy!");
                BecomeCandidate();
                return Candidacy(c);
            }
            else
                return Task.CompletedTask;
        }

        private async Task HeartBeatSend(CancellationToken c)
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
            var retry = TheTrace.LogPolicy(_meAsAPeer.ShortName).RetryForeverAsync();
            var policy = Policy.TimeoutAsync(_settings.ElectionTimeoutMin.Multiply(0.2)).WrapAsync(retry);
            var all = await Task.WhenAll(proxies.Select(p => policy.ExecuteAndCaptureAsync(() => p.AppendEntriesAsync(req))));
            var maxTerm = currentTerm;
            foreach (var r in all)
            {
                if (r.Outcome == OutcomeType.Successful)
                {
                    if (!r.Result.IsSuccess)
                        TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] Got this reason for unsuccessful AppendEntriesAsync from a peer: {r.Result.Reason}");

                    // NOTE: We do NOT change leadership if they send higher term, since they could be candidates whom will not become leaders
                    // we actually do not need to do anything with the result other than logging it
                    if (r.Result.CurrentTerm > maxTerm)
                        maxTerm = r.Result.CurrentTerm;
                }
            }

            if (maxTerm > State.CurrentTerm)
                TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] Revolution brewing. Terms as high as {maxTerm} (vs my {currentTerm}) were seen.");

            _lastHeartbeatSent.Set();
        }

        private async Task Candidacy(CancellationToken c)
        {
            while (_role == Role.Candidate)
            {
                var forMe = 1; // vote for yourself
                var againstMe = 0;

                var peers = _peerManager.GetPeers().ToArray();
                var concensus = (peers.Length / 2) + 1;
                var proxies = peers.Select(x => _peerManager.GetProxy(x.Address));
                var retry = TheTrace.LogPolicy(_meAsAPeer.ShortName).WaitAndRetryAsync(3, (i) => TimeSpan.FromMilliseconds(i * i * 30));
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
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Result of the candidacy for term {State.CurrentTerm}. I got rejected with {againstMe} votes :/");
                    BecomeFollower(maxTerm);
                }
                else if (forMe >= concensus)
                {
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Result of the candidacy for term {State.CurrentTerm}. I got elected with {forMe} votes! :)");
                    BecomeLeader();
                }
                else
                {
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Result of the candidacy for term {State.CurrentTerm}. Non-conclusive with {forMe} for me and {againstMe} against me.");
                }
            }
        }

        private async Task LogCommit(CancellationToken c)
        {
            // IMPORTANTE!! SIEMPERE CREATE VARIABLES LOCALES
            var commitIndex = _volatileState.CommitIndex;
            var lastApplied = _volatileState.LastApplied;

            // NOTE: if snapshotting, it should not commit anything
            if (!_isSnapshotting && commitIndex > lastApplied)
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Applying to the state maquina {commitIndex - lastApplied} entries");

                // TODO: ADD BATCHING LATER
                await _stateMachine.ApplyAsync(
                    _logPersister.GetEntries(lastApplied + 1, (int)(commitIndex - lastApplied))
                        .Select(x => x.Body).ToArray()
                    );

                _volatileState.LastApplied = commitIndex;
            }
        }

        private async Task CreateSnapshot(CancellationToken c)
        {
            if (_isSnapshotting)
                _isSnapshotting = false; // previously crashed perhaps

            var commitIndex = _volatileState.CommitIndex;
            var term = State.CurrentTerm;
            var safeIndex = _volatileState.LastApplied; // last applied is behind LogCommit and logcommit behind NextIndex(s)

            // NOTE: Here used to be a code looking at the minimum LastIndex and set SafeIndex to 
            // min(thatValue, commitIndex) if this was a Leader for the benefit of those 
            // peers that were so behind. But removed since these peers will be using Snapshots

            if (safeIndex - _logPersister.LogOffset < _settings.MinSnapshottingIndexInterval)
                return; // no work

            _isSnapshotting = true;
            try
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Considering snapshotting. SafeIndex: {safeIndex} | LogOffset: {_logPersister.LogOffset}");
                var stream = _snapshotOperator.GetNextSnapshotStream(safeIndex, term);
                await _stateMachine.WriteSnapshotAsync(stream);
                stream.Close();
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Successfully created snapshot for safeIndex {safeIndex}");
                _snapshotOperator.FinaliseSnapshot(safeIndex, term); // this changes LogOffset
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Successfully finalised snapshot for safeIndex {safeIndex}");
                _logPersister.ApplySnapshot(safeIndex + 1); // if this fails then next time it will be cleaned
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Successfully applied snapshot for safeIndex {safeIndex}");
                _snapshotOperator.CleanSnapshots();
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Successfully created and applied snapshot for SafeIndex: {safeIndex}");

                SuccessfulSnapshotCreations++;
            }
            finally
            {
                _isSnapshotting = false;            
            }
        }

        private Func<CancellationToken, Task> PeerAppendLog(Peer peer)
        {
            return (CancellationToken c) =>
            {
                long nextIndex;
                long matchIndex;

                var hasMatch = _volatileLeaderState.TryGetMatchIndex(peer.Id, out matchIndex);
                var hasNext = _volatileLeaderState.TryGetNextIndex(peer.Id, out nextIndex);
                var myLastIndex = _logPersister.LastIndex;

                if (!hasMatch)
                {
                    TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] Could not find peer with id {peer.Id} and address {peer.Address} in matchIndex dic.");
                    return Task.CompletedTask;
                }

                if (!hasNext)
                {
                    TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] Could not find peer with id {peer.Id} and address {peer.Address} in nextIndex dic.");
                    return Task.CompletedTask;
                }

                if (nextIndex > myLastIndex)
                {
                    TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] PeerAppendLog - Nothing to do for peer {peer.ShortName} since myIndex is {myLastIndex} and nextIndex is {nextIndex}.");
                    return Task.CompletedTask; // nothing to do
                }

                var count = (int)Math.Min(_settings.MaxNumberLogEntriesToAskToBeAppended, myLastIndex + 1 - nextIndex);
                var proxy = _peerManager.GetProxy(peer.Address);
                var retry = TheTrace.LogPolicy(_meAsAPeer.ShortName).WaitAndRetryAsync(2, (i) => TimeSpan.FromMilliseconds(20));
                var policy = Policy.TimeoutAsync(_settings.CandidacyTimeout).WrapAsync(retry); // TODO: create its own timeout

                if (nextIndex >= _logPersister.LogOffset)
                {
                    TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] Intending to do SendLog for peer {peer.Address} with nextIndex {nextIndex} and count {count}.");
                    return SendLogs(proxy, policy, peer, nextIndex, matchIndex, count);
                }
                else
                {
                    TheTrace.TraceVerbose($"[{_meAsAPeer.ShortName}] Intending to do SendSnapshot for peer {peer.Address}.");
                    return SendSnapshot(proxy, policy, peer, nextIndex, matchIndex);
                }
            };
        }

        private async Task SendSnapshot(
            IRaftServer proxy,
            AsyncPolicy policy,
            Peer peer,
            long nextIndex,
            long matchIndex)
        {
            var logOffset = LogPersister.LogOffset;
            var term = State.CurrentTerm;
            Snapshot ss;
            if (!SnapshotOperator.TryGetLastSnapshot(out ss))
                throw new InvalidProgramException($"WE DO NOT HAVE A SNAPSHOT for client {peer.Address} whose nextIndex is {nextIndex} yet our LogOffset is {logOffset}");

            if (ss.LastIncludedIndex + 1 < nextIndex)
                throw new InvalidProgramException($"WE DO NOT HAVE A <<PROPER>> SNAPSHOT for client {peer.Address} whose nextIndex is {nextIndex} yet our LogOffset is {logOffset}. Snapshot was have ({ss.FullName}) is short {ss.LastIncludedIndex}");

            // make a copy since it might be cleaned up or opened by another thread for another client
            var fileName = Path.GetTempFileName();
            File.Copy(ss.FullName, fileName, true);
            
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] About to send Snapshot copy file {fileName} to [{peer.ShortName}] and copy of {ss.FullName}.");

            using (var fs = new FileStream(fileName, FileMode.Open))
            {
                var start = 0;
                var total = 0;
                var length = fs.Length;
                var buffer = new byte[_settings.MaxSnapshotChunkSentInBytes];
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Snapshot copy file size is {length}. Location is {fileName} and copy of {ss.FullName}.");
                while (total < length)
                {
                    var count = fs.Read(buffer, 0, buffer.Length);
                    total += count;
                    var result = await proxy.InstallSnapshotAsync(new InstallSnapshotRequest()
                    {
                        CurrentTerm = term,
                        Data = count == buffer.Length ? buffer : buffer.Take(count).ToArray(),
                        LastIncludedIndex = ss.LastIncludedIndex,
                        LastIncludedTerm = term,
                        IsDone = total == length,
                        LeaderId = State.Id,
                        Offset = start
                    });

                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Sent snapshot for peer {peer.Address} with {count} bytes totalling {total}.");

                    start += count;
                    if (result.CurrentTerm != term)
                        TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] I am sending snapshot but this peer {peer.Address} has term {result.CurrentTerm} vs my started term {term} and current term {State.CurrentTerm}.");
                }
            }

            _volatileLeaderState.SetMatchIndex(peer.Id, ss.LastIncludedIndex);
            _volatileLeaderState.SetNextIndex(peer.Id, ss.LastIncludedIndex + 1); // the rest will be done by sending logs
            File.Delete(fileName);
        }

        private async Task SendLogs(
            IRaftServer proxy,
            AsyncPolicy policy,
            Peer peer,
            long nextIndex,
            long matchIndex,
            int count)
        {
            var previousIndexTerm = -1L;
            if (nextIndex > 0)
            {
                if (nextIndex > _logPersister.LogOffset)
                {
                    previousIndexTerm = _logPersister.GetEntries(nextIndex-1, 1).First().Term;
                }
                else
                {
                    Snapshot ss;
                    if (_snapshotOperator.TryGetLastSnapshot(out ss))
                        previousIndexTerm = ss.LastIncludedTerm;
                }
            }

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
                    _volatileLeaderState.SetMatchIndex(peer.Id, nextIndex + count - 1);
                    _volatileLeaderState.SetNextIndex(peer.Id, nextIndex + count);
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Successfully transferred {count} entries from index {nextIndex} to peer {peer.Address} - Next Index is {_volatileLeaderState.NextIndices[peer.Id]}");
                    UpdateCommitIndex();
                }
                else
                {
                    // log reason only
                    TheTrace.TraceWarning($"AppendEntries for start index {nextIndex} and count {count} for peer {peer.Address} with address {peer.Address} in term {State.CurrentTerm} failed with reason type {result.Result.ReasonType} and this reason: {result.Result.Reason}");

                    if (result.Result.ReasonType == ReasonType.LogInconsistency)
                    {
                        var diff = nextIndex - (matchIndex + 1);
                        nextIndex = diff > _settings.MaxNumberOfDecrementForLogsThatAreBehind ?
                            nextIndex - _settings.MaxNumberOfDecrementForLogsThatAreBehind :
                            nextIndex - diff;

                        _volatileLeaderState.SetNextIndex(peer.Id, nextIndex);
                        TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Updated (decremented) next index for peer {peer.Address} to {nextIndex}");
                    }
                }
            }
            else
            {
                // NUNCA!!
                // not interested in network, etc errors, they get logged in the policy
            }
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
            while (index <= _logPersister.LastIndex && index <= majorityIndex && _logPersister.GetEntries(index, 1).First().Term == State.CurrentTerm)
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

            if (request.Entries != null && request.Entries.Length > 0)
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Received the dough {request.Entries.Length} for position after {request.PreviousLogIndex}");
            }


            string message = null;
            lock(State)
            {
                if (request.CurrentTerm > State.CurrentTerm)
                    BecomeFollower(request.CurrentTerm);
            }

            // Reply false if term < currentTerm (§5.1)
            if (request.CurrentTerm < State.CurrentTerm)
            {
                message = $"[{_meAsAPeer.ShortName}] Leader's term is behind ({request.CurrentTerm} vs {State.CurrentTerm}).";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.TermInconsistency, message));
            }

            if (request.Entries == null || request.Entries.Length == 0) // it is a heartbeat, set the leader address
            {
                _leaderAddress = _peerManager.GetPeers().Where(x => x.Id == request.LeaderId).FirstOrDefault()?.Address;
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, true));
            }

            // chaos only when has entries
            _chaos.WreakHavoc();

            if (request.PreviousLogIndex > _logPersister.LastIndex)
            {
                message = $"[{_meAsAPeer.ShortName}] Position for last log entry is {_logPersister.LastIndex} but got entries starting at {request.PreviousLogIndex}";
                TheTrace.TraceWarning(message);
                return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.LogInconsistency, message));
            }

            if (request.PreviousLogIndex < _logPersister.LastIndex)
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Position for PreviousLogIndex {request.PreviousLogIndex} but my LastIndex {_logPersister.LastIndex}");
                
                // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
                var entry = _logPersister.GetEntries(request.PreviousLogIndex, 1).First();
                if (entry.Term != request.CurrentTerm)
                {
                    message = $"[{_meAsAPeer.ShortName}] Position at {request.PreviousLogIndex} has term {entry.Term} but according to leader {request.LeaderId} it must be {request.PreviousLogTerm}";
                    TheTrace.TraceWarning(message);
                    return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, false, ReasonType.LogInconsistency, message));
                }

                // If an existing entry conflicts with a new one(same index but different terms), delete the existing entry and all that follow it(§5.3)
                _logPersister.DeleteEntries(request.PreviousLogIndex + 1);
                TheTrace.TraceWarning($"[{_meAsAPeer.ShortName}] Stripping the log from index {request.PreviousLogIndex + 1}. Last index was {_logPersister.LastIndex}");
            }

            var entries = request.Entries.Select(x => new LogEntry()
            {
                Body = x,
                Term = request.CurrentTerm
            }).ToArray();

            // Append any new entries not already in the log
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Current last index is {_logPersister.LastIndex}. About to append {entries.Length} entries at {request.PreviousLogIndex + 1}");
            _logPersister.Append(entries, request.PreviousLogIndex + 1);

            //If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.LeaderCommitIndex > _volatileState.CommitIndex)
            {
                _volatileState.CommitIndex = Math.Min(request.LeaderCommitIndex, _logPersister.LastIndex);
            }

            message = $"[{_meAsAPeer.ShortName}] Appended {request.Entries.Length} entries at position {request.PreviousLogIndex + 1}";
            TheTrace.TraceInformation(message);
            return Task.FromResult(new AppendEntriesResponse(State.CurrentTerm, true, ReasonType.None, message));
        }

        /// <inheritdoc />
        public async Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
        {
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - was asked to install snapshot {request.LastIncludedIndex} from {request.Offset} offset and isDone={request.IsDone}");
            lock (State)
            {
                if (request.CurrentTerm > State.CurrentTerm)
                    BecomeFollower(request.CurrentTerm);
            }

            try
            {
                _isSnapshotting = true;
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - before write ss {request.LastIncludedIndex} ");            
                _snapshotOperator.WriteLeaderSnapshot(request.LastIncludedIndex, request.LastIncludedTerm, request.Data, request.Offset, request.IsDone);
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - After write ss {request.LastIncludedIndex} ");          

                if (request.IsDone)
                {
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - before apply ss {request.LastIncludedIndex} ");                                    
                    _logPersister.ApplySnapshot(request.LastIncludedIndex + 1);
                    Snapshot ss;
                    if (!_snapshotOperator.TryGetLastSnapshot(out ss) || ss.LastIncludedIndex != request.LastIncludedIndex)
                        throw new InvalidOperationException($"Where did this finalised snapshot with index {request.LastIncludedIndex} go??");
                    
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - before RebuildFromSnapshotAsync ss {request.LastIncludedIndex} ");                   
                    await _stateMachine.RebuildFromSnapshotAsync(ss);
                    _volatileState.LastApplied = ss.LastIncludedIndex; // IMPORTANTE !!
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] InstallSnapshotAsync - after RebuildFromSnapshotAsync ss {request.LastIncludedIndex} ");                      
                    
                    SuccessfulSnapshotInstallations++;
                }

                return new InstallSnapshotResponse() { CurrentTerm = State.CurrentTerm };
            }
            finally
            {
                _isSnapshotting = false;
            }
        }

        /// <inheritdoc />
        public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
        {            
            var peers = _peerManager.GetPeers();
            var peer = peers.Where(x => x.Id == request.CandidateId).FirstOrDefault();
            var peerName = peer?.ShortName ?? request.CandidateId.ToString(); 
            
            lock (State)
            {
                if (request.CurrentTerm > State.CurrentTerm)
                    BecomeFollower(request.CurrentTerm);

                // Reply false if term < currentTerm
                if (State.CurrentTerm > request.CurrentTerm)
                {
                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Rejecting vote of {peerName} due to backward term");
                    return Task.FromResult(new RequestVoteResponse()
                        {
                            CurrentTerm = State.CurrentTerm,
                            VoteGranted = false
                        });
                }
                    

                // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote(§5.2, §5.4)
                if (!State.LastVotedForId.HasValue && _logPersister.LastIndex <= request.LastLogIndex)
                {
                    State.LastVotedForId = request.CandidateId;

                    // If election timeout elapses without receiving AppendEntries RPC from current leader OR GRANTING VOTE TO CANDIDATE: convert to candidate
                    _lastHeartbeat.Set();

                    TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Voting for {peerName} for term {request.CurrentTerm}");
                    return Task.FromResult(new RequestVoteResponse()
                    {
                        CurrentTerm = State.CurrentTerm,
                        VoteGranted = true
                    });
                }

                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Rejecting vote of {peerName} for term {request.CurrentTerm} as it did not fulfil");

                // assume the rest we send back no
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTerm = State.CurrentTerm,
                    VoteGranted = false
                });
            }
        }

        /// <inheritdoc />
        public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
        {

            if (Role == Role.Leader)
            {
                _commands.TryAdd(command);
                return Task.FromResult(new StateMachineCommandResponse() { Outcome = CommandOutcome.Accepted });
            }
            else if (_settings.ExecuteStateMachineCommandsOnClientBehalf && _leaderAddress != null)
            {
                var leaderProxy = _peerManager.GetProxy(_leaderAddress);
                return leaderProxy.ApplyCommandAsync(command);
            }
            else if (_settings.RedirectStateMachineCommands && _leaderAddress != null)
            {
                return Task.FromResult(new StateMachineCommandResponse()
                {
                    Outcome = CommandOutcome.Redirect,
                    DirectTo = _leaderAddress
                });
            }
            else
            {
                return Task.FromResult(new StateMachineCommandResponse() { Outcome = CommandOutcome.ServiceUnavailable });
            }
        }


        #endregion

        #region Role Changes

        protected void OnRoleChanged(Role role)
        {
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Role changed to {role}");
            _commands = new BlockingCollection<StateMachineCommandRequest>(); // reset commands
            RoleChanged?.Invoke(this, new RoleChangedEventArgs(role));
        }

        private void BecomeFollower(long term)
        {
            State.LastVotedForId = null;
            DestroyPeerAppendLogJobs();
            _lastHeartbeat.Set(); // important not to become candidate again at least for another timeout
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] About to set term from {State.CurrentTerm} to {term}");
            State.CurrentTerm = term;
            OnRoleChanged(_role = Role.Follower);
        }

        private void BecomeLeader()
        {
            _leaderAddress = null;
            State.LastVotedForId = State.Id;
            _volatileLeaderState = new VolatileLeaderState();
            var peers = _peerManager.GetPeers().ToArray();
            foreach (var peer in peers)
            {
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] setting up indices for peer {peer.Address}");
                _volatileLeaderState.SetNextIndex(peer.Id, _logPersister.LastIndex + 1);
                _volatileLeaderState.SetMatchIndex(peer.Id, -1L);
            }

            SetupPeerAppendLogJobs(peers);
            OnRoleChanged(_role = Role.Leader);
        }

        private void BecomeCandidate()
        {
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] BecomeCandidate start");
            State.IncrementTerm();
            State.LastVotedForId = State.Id;
            _leaderAddress = null;
            DestroyPeerAppendLogJobs();
            OnRoleChanged(_role = Role.Candidate);
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] BecomeCandidate end");
        }

        private void SetupPeerAppendLogJobs(IEnumerable<Peer> peers)
        {
            foreach (var w in _workers.GetWorkers(Queues.PeerAppendLog))
                w.Start();

            _workers.GetWorkers(Queues.ProcessCommandQueue).Single().Start();

            foreach (var p in peers)
            {
                var localP = p;
                var q = Queues.PeerAppendLog + localP.Address;
                TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] setting up peer append log for queue {q}");
                var todo = PeerAppendLog(localP);

                _workers.Enqueue(q,
                    new Job(todo,
                    TheTrace.LogPolicy(_meAsAPeer.ShortName).WaitAndRetryAsync(3, (i) => TimeSpan.FromMilliseconds(i * i * 50)),
                    TimeSpan.FromMilliseconds(30)));
            }

            // Applying commands received from the clients 
            Func<CancellationToken, Task> pcq = ProcessCommandsQueue;
            _workers.Enqueue(Queues.ProcessCommandQueue,
                new Job(pcq,
                TheTrace.LogPolicy(_meAsAPeer.ShortName).RetryForeverAsync(),
                _settings.ElectionTimeoutMin.Multiply(0.2)));

        }

        private void DestroyPeerAppendLogJobs()
        {
            foreach (var w in _workers.GetWorkers(Queues.PeerAppendLog))
                w.Stop();
            _workers.GetWorkers(Queues.ProcessCommandQueue).Single().Stop();
        }

        #endregion

        public void Dispose()
        {
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Disposing server.");
            _workers.Stop();
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Disposing server. Workers stopped.");
            _logPersister.Dispose();
            TheTrace.TraceInformation($"[{_meAsAPeer.ShortName}] Disposing server. Log Persister stopped.");
        }
    }
}
