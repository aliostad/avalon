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
               TheTrace.LogPolicy().RetryForeverAsync(), (n) => { ran = true; });

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
                ran = true;
                return Task.FromResult(2 + 2);
            },
               TheTrace.LogPolicy().RetryForeverAsync()
            );

            maths.Start();
            maths.Enqueue(job);
            Thread.Sleep(1000);
            Assert.True(ran);
        }

        [Fact]
        public async Task CanAdd2And2ThousandTimes()
        {
            
            var maths = new Worker("maths");
            var ran = 0;
            var job = new Job<int>(c =>
            {
                ran++;
                return Task.FromResult(2 + 2);
            },
                TheTrace.LogPolicy()
                .RetryForeverAsync()
            );

            maths.Start();
            for (int i = 0; i < 1000; i++)
            {
                maths.Enqueue(job);
            }

            Thread.Sleep(10000);
            Assert.Equal(1000, ran);
        }

    }
}
