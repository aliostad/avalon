using Avalon.Raft.Core.Scheduling;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Polly.Retry;
using Polly;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Avalon.Raft.Core.Tests
{
    public class SchedulingTests
    {
        [Fact]
        public void CanAdd2And2()
        {
            var maths = new Worker("maths");
            var ran = false;
            var job = new Job<int>(c => Task.FromResult(2 + 2), 
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

        [Fact]
        public void CanAdd2And2EvenIfItFails()
        {
            var maths = new Worker("maths");
            var ran = false;
            var timesForExceptions = 4;
            var job = new Job<int>(c => 
            {
                if (timesForExceptions-- > 0)
                    throw new ApplicationException();

                return Task.FromResult(2 + 2);
            },
                Policy
                .Handle<Exception>()
                .OrResult<int>((result) =>
                {
                    ran = true;
                    return 4 == result;
                }).RetryForeverAsync()
            );

            maths.Start();
            maths.Enqueue(job);
            Thread.Sleep(1000);
            Assert.True(ran);
        }

    }
}
