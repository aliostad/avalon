using Avalon.Common;
using Avalon.Raft.Core.Persistence;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class LmdbPersisterTests
    {
        private LmdbPersister _persister;
        private readonly string _directory;

        public LmdbPersisterTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _persister = new LmdbPersister(_directory);
        }


        [Fact]
        public void CanLoadInANewPlace()
        {
            Assert.Equal(-1L, _persister.LastIndex);
            Assert.Equal(0L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddLogsHappily()
        {
            LogEntry l = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);

            Assert.Equal(2L, _persister.LastIndex);
            Assert.Equal(0L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddMultiLogsHappily()
        {
            LogEntry l = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { l, l, l, l }, 0);

            Assert.Equal(3L, _persister.LastIndex);
            Assert.Equal(0L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddLogsAndReadThemHappily()
        {
            LogEntry l = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);

            var es = _persister.GetEntries(2, 1);

            Assert.Equal(l.Body, es[0].Body);
        }

        [Fact]
        public void CanAddManyManyLogs()
        {
            int t = Environment.TickCount;
            var r = new Random();
            var buffer = new byte[512];

            for (int i = 0; i < 1000; i++)
            {
                r.NextBytes(buffer);
                LogEntry l = buffer;
                _persister.Append(new[] { l, l, l, l, l, l, l, l, l, l }, i * 10);
            }

            Assert.Equal(10_000 - 1, _persister.LastIndex);
            var taken = Environment.TickCount - t;
            Console.WriteLine(taken);
        }

        [Fact]
        public void CanAddLogsAndDeleteThemHappily()
        {
            LogEntry l = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);
            _persister.Append(new[] { l }, 3);
            _persister.Append(new[] { l }, 4);
            _persister.Append(new[] { l }, 5);

            _persister.DeleteEntries(2L);

            Assert.Equal(1L, _persister.LastIndex);
        }

        [Fact]
        public void StateStoreAndRetrieveWorksLikeACharm()
        {
            var state = new PersistentState()
            {
                CurrentTerm = 42L
            };

            _persister.Save(state);
            var persister = new LmdbPersister(_directory);
            var st2 = persister.Load();
            Assert.Equal(state.CurrentTerm, st2.CurrentTerm);
            Assert.Equal(state.Id, st2.Id);
            Assert.Equal(state.LastVotedForId, st2.LastVotedForId);
            persister.Dispose();
        }

        [Fact]
        public void AppendBadSequencingThrowsUp()
        {
            LogEntry l = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { l }, 0);
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.Append(new[] { l }, 2));
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.Append(new[] { l }, 0));

            Assert.Equal(0L, _persister.LastIndex);
        }

        [Fact]
        public void CanRunSnapshotSimulation()
        {
            const int BatchSize = 100;
            for (int i = 0; i < 1000; i++)
            {
                var entries = Enumerable.Range(i * BatchSize, BatchSize).Select(x => (LogEntry)(Bufferable)Guid.NewGuid()).ToArray();
                _persister.Append(entries, i * BatchSize);
            }

            var lastIncludedIndex = 10_000L - 1;
            var stream = _persister.GetNextSnapshotStream(lastIncludedIndex);
            stream.Write(new byte[1024], 0, 1024);
            stream.Close();
            _persister.FinaliseSnapshot(lastIncludedIndex);
            Assert.Equal(lastIncludedIndex + 1, _persister.LogOffset);

        }

        ~LmdbPersisterTests()
        {
            _persister.Dispose();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            
        }
    }
}
