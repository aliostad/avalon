using Avalon.Raft.Core.Persistence;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class LmdbPersisterTests
    {
        private readonly LmdbPersister _persister;
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


        ~LmdbPersisterTests()
        {
            _persister.Dispose();
            Directory.Delete(_directory, true);
        }
    }
}
