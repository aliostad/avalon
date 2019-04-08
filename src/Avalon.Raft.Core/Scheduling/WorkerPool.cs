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
                w.Start();
            }
        }

        public void Stop()
        {
            foreach (var w in _workers.Values)
            {
                w.Stop();
            }
        }

        public bool IsEmpty(string queueName)
        {
            return _workers[queueName].QueueDepth == 0;
        }

        public void AddWorker(string name)
        {
            _workers.TryAdd(name, new Worker(name));
        }

        public void Enqueue(string queueName, IJob job)
        {
            _workers[queueName].Enqueue(job);
        }

        public bool RemoveWorker(string name)
        {
            Worker w;
            var could = _workers.TryRemove(name, out w);
            if (could)
                w.Stop();
            return could;
        }

        public IEnumerable<Worker> GetWorkers(string nameOrPartOfName)
        {
            foreach(var w in _workers.Values)
            {
                if (w.Name.Contains(nameOrPartOfName))
                    yield return w;
            }
        }
    }
}
