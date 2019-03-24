using Avalon.Raft.Core.Persistence;
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


        public DefaultRaftServerTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _sister = new LmdbPersister(_directory);
            _manijer = new Mock<IPeerManager>();
            _maqina = new Mock<IStateMachine>();
        }

        [Fact]
        public void PerpetualCandidacyWhenDealingWithLazyPeers()
        {
            _manijer.Setup(x => x.GetPeers()).Returns(new[] { "1", "3", "5", "7" }.Select(s => new Peer() { Address = s} ));
            _manijer.Setup(x => x.GetProxy(It.IsAny<string>())).Returns(new LazyPeer());
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, new RaftSettings());

            Thread.Sleep(1000);
            Assert.Equal(Role.Candidate, _server.Role);
        }

        [Fact]
        public void FriendlyPeersWithOneAngryNotABigDeal()
        {
            _manijer.Setup(x => x.GetPeers()).Returns(new[] { "1", "3", "5", "7" }.Select(s => new Peer() { Address = s }));
            _manijer.Setup(x => x.GetProxy(It.IsAny<string>())).Returns(new FriendlyPeer());
            _manijer.Setup(x => x.GetProxy(It.Is<string>(y => y=="7"))).Returns(new AngryPeer());
            _server = new DefaultRaftServer(_sister, _sister, _maqina.Object, _manijer.Object, new RaftSettings());

            Thread.Sleep(1000);
            Assert.Equal(Role.Leader, _server.Role);
        }

        public void Dispose()
        {
            try
            {
                _sister.Dispose();
                Directory.Delete(_directory, true);
            }
            catch (Exception e)
            {
                Trace.TraceWarning(e.ToString());
            }
        }

        class LazyPeer : IRaftServer
        {
            public Role Role => throw new NotImplementedException();

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
            {
                Thread.Sleep(10000);
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

            public event EventHandler<RoleChangedEventArgs> RoleChanged;

            public Task<AppendEntriesResponse> AppendEntriesAsync(AppendEntriesRequest request)
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
                    CurrentTrem = request.CurrentTerm - 1,
                    VoteGranted = true
                });
            }
        }
    }
}
