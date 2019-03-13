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
            var b2 = b.PrefixWithIndexAndTerm(42, 122);
            Assert.Equal(42, BitConverter.ToInt64(b2.Buffer, 0));
            Assert.Equal(122, BitConverter.ToInt64(b2.Buffer, 8));
            Assert.Equal(bb, b2.Buffer.Skip(16));
        }

        [Fact]
        public void CanCreateBufferableFromMultiple()
        {
            var b = new Bufferable(42, Guid.NewGuid(), 1969L);
            Assert.Equal(42, BitConverter.ToInt32(b.Buffer, 0));
            Assert.Equal(1969L, BitConverter.ToInt64(b.Buffer, 4 + 16));
        }

        [Fact]
        public void CanCreateBufferableFromOne()
        {
            var b = new Bufferable(1969L);
            Assert.Equal(1969L, BitConverter.ToInt64(b.Buffer, 0));
        }

        [Fact]
        public void StoredLogEntryIsFunAndImplicitlyConverts()
        {
            var s = new StoredLogEntry() { Body = new byte[] { 1, 2, 3, 4 }, Index = 42L, Term = 122L };
            byte[] buffer = s;
            StoredLogEntry s2 = buffer;

            Assert.Equal(s.Index, s2.Index);
            Assert.Equal(s.Body, s2.Body);
        }
    }
}
