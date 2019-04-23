using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Integration
{
    public class Program
    {
        private static Cluster _cluster;
        private static StreamWriter _log;
        private static int _run;

        public static void Main()
        {
            var stop = false;
            while (!stop)
            {
                _run++;
                var t = DateTime.Now;
                Setup();
                var locum = _run;
                Task.Run(SendCommand(locum));

                // event loop
                while (true)
                {
                    Draw();
                    Thread.Sleep(1000);
                    if (DateTime.Now.Subtract(t).TotalSeconds > 20)
                    {
                        if (_cluster.IsSplitBrain())
                        {
                            Console.WriteLine("BAAAAAAD!!!!");
                            stop = true;
                        }

                        break;
                    }
                }

                if (Console.KeyAvailable)
                    stop = true;

                _cluster.Dispose();
                _log.Close();
            }
        }

        private static Func<Task> SendCommand(int run)
        {
            return async () =>
            {
                var r = new Random();
                while (_run == run)
                {
                    try
                    {
                        for (int i = 0; i < r.Next(1, 10); i++)
                        {
                            var payload = BitConverter.GetBytes(r.Next(0, 100000)).Concat(BitConverter.GetBytes(r.Next(0, 100000))).ToArray();
                            await _cluster.ApplyCommandAsync(new StateMachineCommandRequest()
                            {
                                Command = payload
                            });

                        }
                    }
                    catch (Exception e)
                    {
                        TheTrace.TraceError(e.ToString());
                    }

                    await Task.Delay(r.Next(0, 50));
                }
            };
        }

        private static void Setup()
        {
            var rootPath = "_data";
            if (!Directory.Exists(rootPath))
                Directory.CreateDirectory(rootPath);

            var logFileName = Path.Combine(rootPath, "log.txt");
            _log = new StreamWriter(logFileName);

            TheTrace.Tracer = (level, s, data) =>
            {
                var message = "";
                if (data.Length == 0)
                    message = s;
                else
                    message = string.Format(s, data);
                lock (_log)
                {
                    _log.WriteLine($"{DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff")} {message}");
                }
            };

            _cluster = new Cluster(new ClusterSettings()
            {
                DataRootFolder = rootPath,
                MinSnapshottingIndexInterval = 300
            });

            _cluster.Start();
        }

        private static void Draw()
        {
            Console.Clear();
            Console.WriteLine($"Run {_run}");
            foreach (var address in _cluster.Nodes.Keys.OrderBy(x => x))
            {
                var peer = _cluster.Peers[address];
                var server = _cluster.Nodes[address];
                var message =
                $"{address} ({peer.ShortName})\t{server.Role.ToString()[0]}\t{server.State.CurrentTerm}\t{server.LogPersister.LastIndex}\t{server.VolatileState.CommitIndex}";
                Console.WriteLine(message);
            }
        }
    }
}
