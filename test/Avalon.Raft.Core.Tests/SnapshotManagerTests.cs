using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class SnapshotManagerTests : IDisposable
    {
        private readonly string _directory;
        private readonly SnapshotManager _mgr;

        public SnapshotManagerTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _mgr = new SnapshotManager(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        [Fact]
        public void InEmptyFolderLastSnapIsNull()
        {
            Assert.Null(_mgr.GetLastSnapshot());
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneFinal()
        {
            var index = 42L;
            var term = 2L;
            File.WriteAllText(_mgr.GetFinalFileNameForIndexAndTerm(index, term), "");
            var ss = _mgr.GetLastSnapshot();
            Assert.NotNull(ss);
            Assert.Equal(index, ss.LastIncludedIndex);
            Assert.Equal(term, ss.LastIncludedTerm);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTemp()
        {
            var index = 42L;
            var term = 2L;

            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index, term), "");
            Assert.Null(_mgr.GetLastSnapshot());
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTempOneFinal()
        {
            var index = 42L;
            var term = 2L;
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index + 1, term), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndexAndTerm(index, term), "");
            Assert.Equal(index, _mgr.GetLastSnapshot().LastIncludedIndex);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTempTwoFinal()
        {
            var index = 42L;
            var term = 2L;
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index + 1, term), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndexAndTerm(index, term), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndexAndTerm(index - 1, term), "");
            Assert.Equal(index, _mgr.GetLastSnapshot().LastIncludedIndex);
        }

        [Fact]
        public void IsCleverInFindingOldSnaps()
        {
            var index = 42L;
            var term = 2L;
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index - 1, term), "");
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index + 1, term), "");
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index, term), "");
            File.WriteAllText(_mgr.GetTempFileNameForIndexAndTerm(index - 1, term), "");
            var perv = _mgr.GetPreviousSnapshots();
            Assert.Equal(2, perv.Count());
            foreach (var f in perv)
                Assert.True(_mgr.GetLastSnapshot().LastIncludedIndex < index);

        }
    }
}
