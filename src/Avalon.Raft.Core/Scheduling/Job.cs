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

        bool IsStarted { get; }

        bool IsFinished { get; }

    }

    public class Job : IJob
    {
        private readonly Func<CancellationToken, Task> _work;
        private readonly AsyncPolicy _policy;
        private readonly Action _callback;

        public Exception FinalException { private set; get; }

        public bool IsStarted { get; private set; }

        public bool IsFinished { get; private set; }

        public Job(Func<CancellationToken, Task> work, AsyncPolicy policy, Action callback = null) 
        {
            _work = work;
            _policy = policy;
            _callback = callback;
        }

        public async Task DoAsync(CancellationToken token)
        {
            var result = await _policy.ExecuteAndCaptureAsync(_work, token);
            if (result.Outcome == OutcomeType.Successful)
            {
                _callback?.Invoke();
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
        private readonly AsyncPolicy _policy;
        public bool IsStarted { get; private set; }

        public bool IsFinished { get; private set; }


        public Job(Func<CancellationToken, Task<T>> work, AsyncPolicy policy, Action<T> callback = null)
        {
            _work = work;
            _policy = policy;
            _callback = callback;
        }

        public Exception FinalException { private set; get; }

        public async Task DoAsync(CancellationToken token)
        {
            IsStarted = true;
            var result = await _policy.ExecuteAndCaptureAsync(_work, token);
            if (result.Outcome == OutcomeType.Successful)
            {
                _callback?.Invoke(result.Result);
            }
            else
            {
                FinalException = result.FinalException;
            }

            IsFinished = true;
        }
    }
}
