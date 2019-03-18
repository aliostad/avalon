using Polly;
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
        Task DoAsync(CancellationToken token);

        Exception FinalException { get; }
    }

    public class Job : IJob
    {
        private readonly Func<CancellationToken, Task> _work;
        private readonly AsyncRetryPolicy _policy;

        public Exception FinalException { private set; get; }

        public Job(Func<CancellationToken, Task> work, AsyncRetryPolicy policy) 
        {
            _work = work;
            _policy = policy;
        }

        public async Task DoAsync(CancellationToken token)
        {
            var result = await _policy.ExecuteAndCaptureAsync(_work, token);
            FinalException = result.FinalException;
        }
    }

    public class Job<T> : IJob
    {
        private readonly Func<CancellationToken, Task<T>> _work;
        private readonly Action<T> _callback;
        private readonly AsyncRetryPolicy<T> _policy;

        public Job(Func<CancellationToken, Task<T>> work, AsyncRetryPolicy<T> policy)
        {
            _work = work;
            _policy = policy;
        }

        public Exception FinalException { private set; get; }

        public async Task DoAsync(CancellationToken token)
        {
            var result = await _policy.ExecuteAndCaptureAsync(_work, token);
            FinalException = result.FinalException;
        }
    }
}
