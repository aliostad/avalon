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
        private readonly TimeSpan? _loopDelay = null;

        public Exception FinalException { private set; get; }

        public bool IsStarted { get; private set; }

        public bool IsFinished { get; private set; }

        /// <summary>
        /// Creates a one-off job
        /// </summary>
        /// <param name="work"></param>
        /// <param name="policy"></param>
        /// <param name="callback">Optional</param>
        public Job(Func<CancellationToken, Task> work, AsyncPolicy policy, Action callback = null) 
        {
            _work = work;
            _policy = policy;
            _callback = callback;
        }

        /// <summary>
        /// Creates a looper job that keeps running unless worker is stops
        /// </summary>
        /// <param name="work"></param>
        /// <param name="policy"></param>
        /// <param name="loopDelay"></param>
        /// <param name="callback">Optional</param>
        public Job(Func<CancellationToken, Task> work, AsyncPolicy policy, TimeSpan loopDelay,
                Action callback = null) 
        {
            _work = work;
            _policy = policy;
            _callback = callback;
            _loopDelay = loopDelay;
        }

        public async Task DoAsync(CancellationToken token)
        {
            IsStarted = true;
            while(!token.IsCancellationRequested)
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

                if(_loopDelay == null)
                    break;
                else
                    token.WaitHandle.WaitOne(_loopDelay.Value);
            }

            IsFinished = true;
        }
    }

    public class Job<T> : IJob
    {
        private readonly Func<CancellationToken, Task<T>> _work;
        private readonly Action<T> _callback;
        private readonly AsyncPolicy _policy;
        private readonly TimeSpan? _loopDelay = null;
        public bool IsStarted { get; private set; }

        public bool IsFinished { get; private set; }

        /// <summary>
        /// Creates a one-off job
        /// </summary>
        /// <param name="work"></param>
        /// <param name="policy"></param>
        /// <param name="callback">Optional</param>
        public Job(Func<CancellationToken, Task<T>> work, AsyncPolicy policy, Action<T> callback = null)
        {
            _work = work;
            _policy = policy;
            _callback = callback;
        }

        /// <summary>
        /// Creates a looper job that keeps running unless worker is stops
        /// </summary>
        /// <param name="work"></param>
        /// <param name="policy"></param>
        /// <param name="loopDelay">delay between each time work runs</param>
        /// <param name="callback">Optional</param>
        public Job(Func<CancellationToken, Task<T>> work, AsyncPolicy policy, TimeSpan loopDelay, Action<T> callback = null)
        {
            _work = work;
            _policy = policy;
            _callback = callback;
            _loopDelay = loopDelay;
        }

        public Exception FinalException { private set; get; }

        public async Task DoAsync(CancellationToken token)
        {
            IsStarted = true;
            while(!token.IsCancellationRequested)
            {
                var result = await _policy.ExecuteAndCaptureAsync(_work, token);
                if (result.Outcome == OutcomeType.Successful)
                {
                    _callback?.Invoke(result.Result);
                }
                else
                {
                    FinalException = result.FinalException;
                }

                if(_loopDelay == null)
                    break;
                else
                    token.WaitHandle.WaitOne(_loopDelay.Value);
            }

            IsFinished = true;
        }
    }
}
