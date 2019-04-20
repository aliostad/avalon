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
        private static CancellationToken _cancel;
        private static Cluster _cluster;
        private static StreamWriter _log;


        public static void Main()
        {
            var source = new CancellationTokenSource();
            _cancel = source.Token;
            Setup();

            // event loop
            while(!_cancel.IsCancellationRequested && !Console.KeyAvailable)
            {
                Draw();
                Thread.Sleep(100);
            }

            _cluster.Dispose();
            _log.Close();
        }

        private static void Setup()
        {
            var rootPath = "_data";
            _cluster = new Cluster(new ClusterSettings() {
                DataRootFolder = rootPath
            });

            var logFileName = Path.Combine(rootPath, "log.txt");
            _log = new StreamWriter(logFileName);
            TheTrace.Tracer = (level, s, data) => {
                var message = "";
                if (data.Length == 0)
                    message = s;
                else
                    message = string.Format(s, data);
                lock(_log)
                {
                    _log.WriteLine($"{DateTimeOffset.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff")} {message}");
                }
            };

            _cluster.Start();
        }

        private static void Draw()
        {
            Console.Clear();
            foreach (var address in _cluster.Nodes.Keys.OrderBy(x => x))
            {
                var peer = _cluster.Peers[address];
                var server = _cluster.Nodes[address];
                var message = $"{address} ({peer.ShortName})\t{server.Role.ToString()[0]}\t{server.State.CurrentTerm}";
                Console.WriteLine(message);
            }
        }
    }
}
