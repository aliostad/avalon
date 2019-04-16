using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core
{
    public static class FunctionalComposition
    {
        public static Func<CancellationToken, Task> ComposeOneOff(this Action action)
        {
            return (c) =>
            {
                if (!c.IsCancellationRequested)
                {
                    action();
                }

                return Task.CompletedTask;
            };
        }

        public static Func<CancellationToken, Task> ComposeOneOff(this Func<Task> action)
        {
            return (c) =>
            {
                if (!c.IsCancellationRequested)
                {
                    return action();
                }

                return Task.CompletedTask;
            };
        }

        public static Func<CancellationToken, Task> ComposeLooper(this Action action, TimeSpan timeout, string name)
        {
            return (c) =>
            {
                try
                {
                    while (!c.IsCancellationRequested)
                    {
                        if (!c.WaitHandle.WaitOne(timeout))
                        {
                            action();
                        }
                        else
                        {
                            TheTrace.TraceInformation($"Received a signal so end is nigh - {name}");
                        }
                    }

                    return Task.CompletedTask;
                }
                catch
                {
                    TheTrace.TraceWarning($"Coming OUT OF LOOPER for {name} DUE EXCEPTION. SHOULD NEVER HAPPEN");
                    throw;
                }
            };
        }

        public static Func<CancellationToken, Task> ComposeLooper(this Func<Task> action, TimeSpan timeout, string name)
        {
            return async (c) =>
            {
                try
                {
                    while (!c.IsCancellationRequested)
                    {
                        if (!c.WaitHandle.WaitOne(timeout))
                        {
                            await action();
                        }
                        else
                        {
                            TheTrace.TraceInformation($"Received a signal so end is nigh - {name}");
                        }
                    }
                }
                catch
                {
                    TheTrace.TraceWarning($"Coming OUT OF LOOPER for {name} DUE EXCEPTION. SHOULD NEVER HAPPEN");
                    throw;
                }
            };
        }
    }
}
