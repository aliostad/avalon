using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Scheduling
{
    public class WorkerPool
    {
        private ConcurrentDictionary<string, Worker> _workers = new ConcurrentDictionary<string, Worker>();
        public WorkerPool(params string[] workerNames)
        {
            foreach (var name in workerNames)
            {
                _workers.TryAdd(name, new Worker(name));
            }
        }

        public void Start()
        {
            foreach (var w in _workers.Values)
            {
                w.Stop();
            }
        }

        public void Stop()
        {
            foreach (var w in _workers.Values)
            {
                w.Stop();
            }
        }

        public void AddWorker(string name)
        {
            _workers.TryAdd(name, new Worker(name));
        }

        //public Worker this[string name] => _workers[name];

        public void Enqueue(string name, IJob job)
        {
            _workers[name].Enqueue(job);
        }

        public bool RemoveWorker(string name)
        {
            Worker w;
            var could = _workers.TryRemove(name, out w);
            if (could)
                w.Stop();
            return could;
        }
    }
}
