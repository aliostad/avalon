using Avalon.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class BufferableTests
    {
        [Fact]
        public void ItIsSafeToUseUnsafeCopy()
        {
            var r = new Random();
            var bb = new byte[128];
            r.NextBytes(bb);
            var b = new Bufferable(bb);
            var b2 = b.PrefixWithIndex(42);
            Assert.Equal(42, BitConverter.ToInt64(b2.Buffer, 0));
            Assert.Equal(bb, b2.Buffer.Skip(8));
        }
    }
}
