using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Scheduling
{
    public class Worker : IDisposable
    {
        private readonly BlockingCollection<IJob> _q = new BlockingCollection<IJob>();
        private CancellationTokenSource _cancel;
        private Thread _th;

        public Worker(string name)
        {
            Name = name;
        }

        public bool IsRunning { private set; get; }
        public string Name { private set; get; }

        public void Start()
        {
            if (IsRunning)
                throw new InvalidOperationException("Already running.");

            _cancel = new CancellationTokenSource();
            _th = new Thread(Sisyphus)
            {
                IsBackground = true
            };

            _th.Start();
            IsRunning = true;
        }

        public void Stop()
        {
            if (!IsRunning)
            {
                TheTrace.TraceWarning("Asked to stop while already stopped.");
                return;
            }

            _cancel.Cancel();
            _cancel.Dispose();
            if (!_th.Join(50))
                _th.Abort();

            IsRunning = false;
        }

        public void Enqueue(IJob job)
        {
            _q.Add(job);
        }

        private void Sisyphus()
        {
            while(!_cancel.IsCancellationRequested)
            {
                try
                {
                    var job = _q.Take(_cancel.Token);
                    // yes, running async in sync because cannot leave these to threadpool to do. Too important to do that, cannot afford to face thread exhaustion
                    job.DoAsync(_cancel.Token).GetAwaiter().GetResult(); 
                }
                catch (OperationCanceledException ce)
                {
                    // OK
                }
                catch(Exception e)
                {
                    TheTrace.TraceError($"Job errored: {e}");
                }
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
