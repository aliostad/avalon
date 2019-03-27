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
    }
}
