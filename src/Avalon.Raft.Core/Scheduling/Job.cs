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
        Task DoAsync();

        void Cancel();

        Exception FinalException { get; }
    }

    public class Job : IJob
    {
        private readonly Func<CancellationToken, Task> _work;
        private readonly Action _callback;
        private readonly AsyncRetryPolicy _policy;
        private readonly CancellationTokenSource _cancel;

        public Exception FinalException { private set; get; }

        public Job(Func<CancellationToken, Task> work, Action callback, AsyncRetryPolicy policy)
        {
            _work = work;
            _callback = callback;
            _policy = policy;
            _cancel = new CancellationTokenSource();
        }

        public void Cancel()
        {
            _cancel.Cancel();
        }

        public async Task DoAsync()
        {
            var result = await _policy.ExecuteAndCaptureAsync(_work, _cancel.Token);
            if (result.Outcome == OutcomeType.Successful)
            {
                _callback();
            }
            else
            {
                FinalException = result.FinalException;
            }
        }
    }

    public class Job<T> : IJob
    {
        private readonly Func<CancellationToken, Task<T>> _work;
        private readonly Action<T> _callback;
        private readonly AsyncRetryPolicy<T> _policy;
        private readonly CancellationTokenSource _cancel;

        public Job(Func<CancellationToken, Task<T>> work, Action<T> callback, AsyncRetryPolicy<T> policy)
        {
            _work = work;
            _callback = callback;
            _policy = policy;
            _cancel = new CancellationTokenSource();
        }

        public Exception FinalException { private set; get; }

        public void Cancel()
        {
            _cancel.Cancel();
        }

        public async Task DoAsync()
        {
            var result = await _policy.ExecuteAndCaptureAsync(_work, _cancel.Token);
            if (result.Outcome == OutcomeType.Successful)
            {
                _callback(result.Result);
            }
            else
            {
                FinalException = result.FinalException;
            }
        }
    }
}
