using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Scheduling
{
    public interface IJob
    {
        Task DoAsync();
    }

    public class Job : IJob
    {
        private readonly Func<CancellationToken, Task> _work;
        private readonly Action _callback;
        private readonly RetryPolicy _policy;

        public Job(Func<CancellationToken, Task> work, Action callback, RetryPolicy policy)
        {
            _work = work;
            _callback = callback;
            _policy = policy;
        }

        public Task DoAsync()
        {
            throw new NotImplementedException();
        }
    }

    public class Job<T> : IJob
    {
        private readonly Func<CancellationToken, Task<T>> _work;
        private readonly Action<T> _callback;
        private readonly RetryPolicy _policy;

        public Job(Func<CancellationToken, Task<T>> work, Action<T> callback)
        {
            _work = work;
            _callback = callback;
        }

        public Task DoAsync()
        {
            throw new NotImplementedException();
        }
    }
}
