using System;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Chaos
{
    public class SimpleChaos : IChaos
    {
        private Random _r = new Random();
        private readonly Action _throw;
        private readonly Action _delay;
        private readonly Func<Task> _delayAsync;


        public SimpleChaos(TimeSpan maxDelay, double exceptionProbability = 0.01, double delayProbability = 0.05)
        {
            _throw = () => {
                if (_r.NextDouble() <= exceptionProbability)
                    throw new ChaosException();
            };

            _delay = () => 
            {
                if (_r.NextDouble() <= delayProbability)
                    Thread.Sleep(_r.Next((int) maxDelay.TotalMilliseconds));
            };

            _delayAsync = () => 
            {
                if (_r.NextDouble() <= delayProbability)
                    return Task.Delay(_r.Next((int) maxDelay.TotalMilliseconds));
                else
                    return Task.CompletedTask;
            };

        }

        public void WreakHavoc()
        {
            _delay();
            _throw();
        }

        public async Task WreakHavocAsync()
        {
            await _delayAsync();
            _throw();
        }
    }
}
