using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class OtherTests
    {
        [Fact]
        public void Multiply()
        {
            Assert.Equal(TimeSpan.FromMilliseconds(200).Multiply(0.5), TimeSpan.FromMilliseconds(100));
        }

        [Theory]
        [InlineData(new int[0], -1)]
        [InlineData(new int[] {-1, -1, 123, 42 }, -1)]
        [InlineData(new int[] {-1, 10, 123, 42 }, 10)]
        public void FindsMajorityPerfectly(int[] matchIndices, long majority)
        {
            var s = new VolatileLeaderState();
            foreach (var i in matchIndices)
                s.MatchIndices[Guid.NewGuid()] = i;

            Assert.Equal(majority, s.GetMajorityMatchIndex());
        }
    }
}
