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

        [Theory]
        [InlineData(new[] { "29dc03f2337244b5b9b1b26c4f9494e9", "8d4445618bd94040a14f927d7dfe6f92", "fc07878d73b64c7cb251900d1e8a32a4" }, new[] { "21", "81", "F1" })]
        [InlineData(new[] { "29dc03f2337244b5b9b1b26c4f9494e9", "28dc03f2337244b5b9b1b26c4f9494e9", "fc07878d73b64c7cb251900d1e8a32a4" }, new[] { "22", "21", "F1" })]
        [InlineData(new[] { "29dc03f2337244b5b9b1b26c4f9494e9", "28dc03f2337244b5b9b1b26c4f9494e9", "27dc03f2337244b5b9b1b26c4f9494e9" }, new[] { "23", "22", "21" })]
        public void ChoosesShortNamesLikeAKing(string[] gvids, string[] shortNames)
        {
            var peers = gvids.Select(x => new Peer(x, Guid.Parse(x))).ToArray();
            peers.ChooseShortNames();
            Assert.Equal(shortNames, peers.Select(x => x.ShortName).ToArray());
        }


    }
}
