﻿using Avalon.Raft.Core.Persistence;
using Avalon.Raft.Core.Rpc;
using Moq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class DefaultRaftServerTests : IDisposable
    {
        private readonly string _directory;
        private readonly LmdbPersister _sister;
        private readonly Mock<IPeerManager> _manijer;
        private readonly Mock<IStateMachine> _maqina;
        private DefaultRaftServer _server;
        private readonly object _lock = new object();
        private readonly string _correlationId = Guid.NewGuid().ToString("N");
        private StreamWriter _writer;

        private const bool OutputTraceLog = true;
        

        public DefaultRaftServerTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _sister = new LmdbPersister(_directory);
            _manijer = new Mock<IPeerManager>();
            _maqina = new Mock<IStateMachine>();
            _writer = new StreamWriter(new FileStream($"trace_{_correlationId}.log", FileMode.OpenOrCreate))
            {
                AutoFlush = true
            }; 

            if (OutputTraceLog)
            {
                TheTrace.Tracer = (level, s, os) =>
                {
                    lock (_lock)
                    {
                        var message = $"{DateTime.Now.ToString("yyyy-MM-dd:HH-mm-ss.fff")}\t{_correlationId}\t{level}\t{(os.Length == 0 ? s : string.Format(s, os))}";
                        _writer.WriteLine(message);
                    }
                };
            }
        }

        [Fact]
        public void PerpetualCandidacyWhenDealingWithLazyPeers()
        {
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);

            _manijer.Setup(x => x.GetPeers()).Returns(new[] { "1", "3", "5", "7" }.Select(s => new Peer(s, Guid.NewGuid())));
            _manijer.Setup(x => x.GetProxy(It.IsAny<string>())).Returns(new LazyPeer());
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            Thread.Sleep(2000);
            Assert.Equal(Role.Candidate, _server.Role);
        }

        [Fact]
        public void FriendlyPeersWithOneAngryNotABigDeal()
        {
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);

            _manijer.Setup(x => x.GetPeers()).Returns(new[] { "1", "3", "5", "7" }.Select(s => new Peer(s, Guid.NewGuid())));
            _manijer.Setup(x => x.GetProxy(It.IsAny<string>())).Returns(new FriendlyPeer());
            _manijer.Setup(x => x.GetProxy(It.Is<string>(y => y=="7"))).Returns(new AngryPeer());
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            Thread.Sleep(1000);
            Assert.Equal(Role.Leader, _server.Role);
        }

        [Fact]
        public void BackStabberPeersMakeYouFollower()
        {
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _manijer.Setup(x => x.GetPeers()).Returns(new[] { "1", "3", "5", "7" }.Select(s => new Peer(s, Guid.NewGuid())));
            _manijer.Setup(x => x.GetProxy(It.IsAny<string>())).Returns(new BackStabberPeer());
            _manijer.Setup(x => x.GetProxy(It.Is<string>(y => y == "7"))).Returns(new FriendlyPeer());
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            TheTrace.TraceInformation("OK, now this is before wait...");
            Thread.Sleep(300);
            TheTrace.TraceInformation("Wait finished.");
            Assert.True(1 >= _server.State.CurrentTerm);
            TheTrace.TraceInformation("Checked Term.");
            Assert.True(Role.Follower == _server.Role || Role.Candidate == _server.Role);
            TheTrace.TraceInformation("Checked Role.");
        }

        [Fact]
        public async Task KeepFeedingAFollowerAndNeverDreamsOfPower()
        {
            var t = new CancellationTokenSource();
            var leaderId = Guid.NewGuid();
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            // to set the term to 1
            await _server.AppendEntriesAsync(new AppendEntriesRequest()
            {
                CurrentTerm = 1,
                Entries = new byte[0][],
                LeaderCommitIndex = 20,
                LeaderId = leaderId,
                PreviousLogIndex = -1,
                PreviousLogTerm = 0
            });

            _server.LastHeartBeat = new AlwaysRecentTimestamp();
            _server.LastHeartBeatSent = new AlwaysRecentTimestamp();

            TheTrace.TraceInformation("OK, now this is before wait...");
            Thread.Sleep(1000);
            TheTrace.TraceInformation("Wait finished.");
            Assert.Equal(1, _server.State.CurrentTerm);
            TheTrace.TraceInformation("Checked Term.");
            Assert.Equal(Role.Follower, _server.Role);
            TheTrace.TraceInformation("Checked Role.");
            t.Cancel();
        }


        /// <summary>
        /// Reply false if term < currentTerm (§5.1)
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task NeverVotesYesToLowerTerm()
        {
            var leaderId = Guid.NewGuid();
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            // to set the term to 1
            var result = await _server.RequestVoteAsync(new RequestVoteRequest()
            {
                CandidateId = Guid.NewGuid(),
                CurrentTerm = 1,
                LastLogIndex = 2,
                LastLogTerm = 1
            });

            Thread.Sleep(400); // now it becomes candidate and returns no

            // now its term already moved to 2 due to timeout
            result = await _server.RequestVoteAsync(new RequestVoteRequest()
            {
                CandidateId = Guid.NewGuid(),
                CurrentTerm = 1,
                LastLogIndex = 2,
                LastLogTerm  = 1
            });

            Assert.False(result.VoteGranted);
            Assert.Equal(2, result.CurrentTerm);

        }

        [Fact]
        public async Task VotesYesToTheRightGuy()
        {
            var leaderId = Guid.NewGuid();
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            _server.LastHeartBeat = new AlwaysRecentTimestamp();
            _server.LastHeartBeatSent = new AlwaysRecentTimestamp();

            Thread.Sleep(400);
            var result = await _server.RequestVoteAsync(new RequestVoteRequest()
            {
                CandidateId = Guid.NewGuid(),
                CurrentTerm = 2,
                LastLogIndex = 2,
                LastLogTerm = 1
            });

            Assert.True(result.VoteGranted);
            Assert.Equal(2, result.CurrentTerm);

        }

        [Fact]
        public async Task NeverVotesTwice()
        {
            var leaderId = Guid.NewGuid();
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            _server.LastHeartBeat = new AlwaysRecentTimestamp();
            _server.LastHeartBeatSent = new AlwaysRecentTimestamp();

            Thread.Sleep(400);
            var result = await _server.RequestVoteAsync(new RequestVoteRequest()
            {
                CandidateId = Guid.NewGuid(),
                CurrentTerm = 2,
                LastLogIndex = 2,
                LastLogTerm = 1
            });

            Thread.Sleep(400);
            var result2 = await _server.RequestVoteAsync(new RequestVoteRequest()
            {
                CandidateId = Guid.NewGuid(),
                CurrentTerm = 2,
                LastLogIndex = 2,
                LastLogTerm = 1
            });

            Assert.False(result2.VoteGranted);

        }

        [Fact]
        public async Task RunsErrandsForLogs()
        {
            var leaderId = Guid.NewGuid();
            var settings = new RaftSettings();
            settings.ElectionTimeoutMin = settings.ElectionTimeoutMax = settings.CandidacyTimeout = TimeSpan.FromMilliseconds(200);
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, settings);

            // has to be done - otherwise it becomes a candidate
            _server.LastHeartBeat = new AlwaysRecentTimestamp();
            _server.LastHeartBeatSent = new AlwaysRecentTimestamp();

            var currentPosition = 0;
            var term = 2;
            for (int i = 0; i < 10; i++)
            {
                var entries = GetSomeRandomEntries();
                await _server.AppendEntriesAsync(new AppendEntriesRequest()
                {
                    CurrentTerm = term,
                    Entries = entries,
                    LeaderCommitIndex = currentPosition + entries.Length,
                    LeaderId = leaderId,
                    PreviousLogIndex = currentPosition - 1,
                    PreviousLogTerm = term 
                });

                currentPosition += entries.Length;
            }

            Assert.Equal(currentPosition - 1, _sister.LastIndex);
            Assert.Equal(Role.Follower, _server.Role);
        }

        [Fact]
        public void LeadsFollowersAndTheirLogsLikeALeader()
        {
            
        }

        private byte[][] GetSomeRandomEntries()
        {
            var l = new List<byte[]>();
            var r = new Random();
            for (int i = 0; i < r.Next(10, 200); i++)
            {
                var bb = new byte[r.Next(32, 129)];
                r.NextBytes(bb);
                l.Add(bb);
            }

            return l.ToArray();
        }

        public void Dispose()
        {
            try
            {
                _server.Dispose();
                Thread.Sleep(100);
                _sister.Dispose();
                TheTrace.TraceInformation("about to delete test directory.");
                Directory.Delete(_directory, true);
                TheTrace.TraceInformation("deleted directory.");
                _writer.Close();
            }
            catch (Exception e)
            {
                Trace.TraceWarning(e.ToString());
            }
        }

        #region Peers

        class LazyPeer : IRaftServer
        {
            public Role Role => throw new NotImplementedException();

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
            {
                Thread.Sleep(10000);
                throw new NotImplementedException();
            }

            public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
            {
                throw new NotImplementedException();
            }

            public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
            {
                Thread.Sleep(10000);
                throw new NotImplementedException();
            }

            public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
            {
                Thread.Sleep(10000);
                throw new NotImplementedException();
            }
        }

        class AngryPeer : IRaftServer
        {
            public Role Role => throw new NotImplementedException();

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
            {
                throw new Exception("I am Angry!");
            }

            public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
            {
                throw new NotImplementedException();
            }

            public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
            {
                throw new Exception("I am Angry!");
            }

            public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
            {
                throw new Exception("I am Angry!");
            }
        }

        class FriendlyPeer : IRaftServer
        {
            public Role Role => throw new NotImplementedException();

            public List<AppendEntriesRequest> AllThemAppendEntriesRequests = new List<AppendEntriesRequest>();

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
            {
                throw new NotImplementedException();
            }

            public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
            {
                throw new NotImplementedException();
            }

            public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
            {
                throw new NotImplementedException();
            }

            public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
            {
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTerm = request.CurrentTerm - 1,
                    VoteGranted = true
                });
            }
        }

        class BackStabberPeer : IRaftServer
        {
            public Role Role => throw new NotImplementedException();

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
            {
                throw new NotImplementedException();
            }

            public Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command)
            {
                throw new NotImplementedException();
            }

            public Task<InstallSnapshotResponse> InstallSnapshotAsync(InstallSnapshotRequest request)
            {
                throw new NotImplementedException();
            }

            public Task<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest request)
            {
                return Task.FromResult(new RequestVoteResponse()
                {
                    CurrentTerm = request.CurrentTerm ,
                    VoteGranted = false
                });
            }
        }

        #endregion

    }
}
