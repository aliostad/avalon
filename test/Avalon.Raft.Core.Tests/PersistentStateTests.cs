using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class PersistentStateTests
    {
        [Fact]
        public void ToBytesAndBackGeneratesSimilarObject()
        {
            var s = new PersistentState()
            {
                CurrentTerm = 42L,
                LastVotedForId = Guid.NewGuid()
            };
            var s2 = PersistentState.FromBuffer(s.ToBytes());

            Assert.Equal(s.Id, s2.Id);
            Assert.Equal(s.LastVotedForId, s2.LastVotedForId);
            Assert.Equal(s.CurrentTerm, s2.CurrentTerm);
        }
    }
}
