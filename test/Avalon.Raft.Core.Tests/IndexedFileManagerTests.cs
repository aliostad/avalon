using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class IndexedFileManagerTests
    {
        private readonly string _directory;
        private readonly IndexedFileManager _mgr;

        public IndexedFileManagerTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _mgr = new IndexedFileManager(_directory);
        }

        [Fact]
        public void InEmptyFolderLastIndexIsNull()
        {
            Assert.False(_mgr.GetLastFinalIndex().HasValue);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneFinal()
        {
            var index = 42L;
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index), "");
            Assert.Equal(index, _mgr.GetLastFinalIndex().Value);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTemp()
        {
            var index = 42L;
            File.WriteAllText(_mgr.GetTempFileNameForIndex(index), "");
            Assert.False(_mgr.GetLastFinalIndex().HasValue);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTempOneFinal()
        {
            var index = 42L;
            File.WriteAllText(_mgr.GetTempFileNameForIndex(index + 1), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index), "");
            Assert.Equal(index, _mgr.GetLastFinalIndex().Value);
        }

        [Fact]
        public void IsCleverInFindingLastWhenOneExistsWithOneTempTwoFinal()
        {
            var index = 42L;
            File.WriteAllText(_mgr.GetTempFileNameForIndex(index + 1), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index - 1), "");
            Assert.Equal(index, _mgr.GetLastFinalIndex().Value);
        }

        [Fact]
        public void IsCleverInFindingOldSnaps()
        {
            var index = 42L;
            File.WriteAllText(_mgr.GetTempFileNameForIndex(index - 1), "");
            File.WriteAllText(_mgr.GetTempFileNameForIndex(index + 1), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index), "");
            File.WriteAllText(_mgr.GetFinalFileNameForIndex(index - 1), "");
            var perv = _mgr.GetPreviousSnapshots();
            Assert.Equal(2, perv.Count());
            foreach (var f in perv)
                Assert.True(_mgr.GetSnapshotIndex(f) < index);

        }

        ~IndexedFileManagerTests()
        {
            Directory.Delete(_directory, true);
        }
    }
}
