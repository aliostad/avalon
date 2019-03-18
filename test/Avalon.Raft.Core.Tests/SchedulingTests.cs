using Avalon.Raft.Core.Scheduling;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Polly.Retry;
using Polly;
using System.Threading;

namespace Avalon.Raft.Core.Tests
{
    public class SchedulingTests
    {
        [Fact]
        public void CanAdd2And2()
        {
            var maths = new Worker("maths");
            var ran = false;
            var job = new Job<int>( async (c) => 2 + 2, 
                Policy.HandleResult<int>((result) =>
                {
                    ran = true;
                    return 4 == result;
                }).RetryForeverAsync<int>()
            );

            maths.Start();
            maths.Enqueue(job);
            Thread.Sleep(100);
            Assert.True(ran);
        }

    }
}
