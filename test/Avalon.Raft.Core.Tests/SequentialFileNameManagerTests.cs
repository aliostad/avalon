using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class SequentialFileNameManagerTests
    {
        private string _directory;
        private const string BaseFileName = "shamoorti";

        public SequentialFileNameManagerTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        [Fact]
        public void Sequence_Is_Zero_At_First()
        {
            var s = new SequentialFileNameManager(_directory, BaseFileName);
            Assert.Equal(0, s.CurrentSequence);
        }

        [Fact]
        public void Sequence_Increases()
        {
            var s = new SequentialFileNameManager(_directory, BaseFileName);
            File.WriteAllText(s.CurrentFullFileName, "");
            File.WriteAllText(s.NextFullFileName, "");
            s = new SequentialFileNameManager(_directory, BaseFileName);
            Assert.Equal(1, s.CurrentSequence);
        }

        [Fact]
        public void Sequence_Increases_2()
        {
            var s = new SequentialFileNameManager(_directory, BaseFileName);
            File.WriteAllText(s.CurrentFullFileName, "");
            File.WriteAllText(s.NextFullFileName, "");
            s = new SequentialFileNameManager(_directory, BaseFileName);
            Assert.Equal(2, s.Increment());
        }

        [Fact]
        public void Clear_Removes_All_But_Current()
        {
            var s = new SequentialFileNameManager(_directory, BaseFileName);
            File.WriteAllText(s.CurrentFullFileName, "");
            File.WriteAllText(s.NextFullFileName, "");
            s = new SequentialFileNameManager(_directory, BaseFileName);
            s.Increment();
            File.WriteAllText(s.CurrentFullFileName, "");
            Assert.Equal(3, Directory.GetFiles(_directory).Length);
            s.Clear();
            Assert.Equal(1, Directory.GetFiles(_directory).Length);
        }

        ~SequentialFileNameManagerTests()
        {
            Directory.Delete(_directory, true);
        }
    }
}
