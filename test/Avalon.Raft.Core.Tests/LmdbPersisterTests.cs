using Avalon.Common;
using Avalon.Raft.Core.Persistence;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Avalon.Raft.Core.Tests
{
    public class LmdbPersisterTests : IDisposable
    {
        private LmdbPersister _persister;
        private readonly string _directory;
        private StreamWriter _writer;
        private readonly object _lock = new object();
        private readonly string _correlationId = Guid.NewGuid().ToString("N");

        private const bool OutputTraceLog = false;

        public LmdbPersisterTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _persister = new LmdbPersister(_directory);
            _writer = new StreamWriter(new FileStream($"trace_{_correlationId}.log", FileMode.OpenOrCreate))
            {
                AutoFlush = true
            };

            if (OutputTraceLog)
            {
                TheTrace.Tracer = (level, s, os) =>
                {
                    lock (_lock)
                    {
                        var message = $"{DateTime.Now.ToString("yyyy-MM-dd:HH-mm-ss.fff")}\t{_correlationId}\t{level}\t{(os.Length == 0 ? s : string.Format(s, os))}";
                        try
                        {
                            _writer.WriteLine(message);
                        }
                        catch
                        {
                        }
                    }
                };
            }

        }


        [Fact]
        public void CanLoadInANewPlace()
        {
            Assert.Equal(-1L, _persister.LastIndex);
            Assert.Equal(-1L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddLogsHappily()
        {
            LogEntry l = new LogEntry()
            {
                Body = new byte[] { 1, 2, 3, 4 },
                Term = 42L
            };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);

            Assert.Equal(2L, _persister.LastIndex);
            Assert.Equal(-1L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddMultiLogsHappily()
        {
            LogEntry l = new LogEntry()
            {
                Body = new byte[] { 1, 2, 3, 4 },
                Term = 42L
            };

            _persister.Append(new[] { l, l, l, l }, 0);

            Assert.Equal(3L, _persister.LastIndex);
            Assert.Equal(-1L, _persister.LogOffset);
        }

        [Fact]
        public void CanAddLogsAndReadThemHappily()
        {
            var buffer = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 0L } }, 0);
            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 1L } }, 1);
            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 2L } }, 2);

            var es = _persister.GetEntries(1, 1);

            Assert.Equal(buffer, es[0].Body);
            Assert.Equal(1L, es[0].Term);
        }

        [Fact]
        public void CanAddLogsAndReadUnHappilyWhenRangeWrongy()
        {
            var buffer = new byte[] { 1, 2, 3, 4 };

            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 0L } }, 0);
            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 1L } }, 1);
            _persister.Append(new[] { new LogEntry() { Body = buffer, Term = 2L } }, 2);
            Assert.Equal(2L, _persister.LastIndex);
            Assert.Equal(2L, _persister.LastEntryTerm);

            Assert.ThrowsAny<InvalidOperationException>(() => _persister.GetEntries(3, 1));
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.GetEntries(2, 2));
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.GetEntries(0, 4));
        }

        [Fact]
        public void CanAddManyManyLogs()
        {
            int t = Environment.TickCount;
            var r = new Random();
            var buffer = new byte[128];

            for (int i = 0; i < 1000; i++)
            {
                r.NextBytes(buffer);
                LogEntry l = new LogEntry()
                {
                    Body = buffer,
                    Term = 42L
                };
                
                _persister.Append(new[] { l, l, l, l, l, l, l, l, l, l }, i * 10);
            }

            Assert.Equal(10_000 - 1, _persister.LastIndex);
            var taken = Environment.TickCount - t;
            Console.WriteLine(taken);
        }

        [Fact]
        public void CanAddLogsAndDeleteThemHappily()
        {
            LogEntry l = new LogEntry()
            {
                Body = new byte[] { 1, 2, 3, 4 },
                Term = 42L
            };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);
            _persister.Append(new[] { l }, 3);
            _persister.Append(new[] { l }, 4);
            _persister.Append(new[] { l }, 5);

            _persister.DeleteEntries(2L);

            Assert.Equal(1L, _persister.LastIndex);
        }

        [Fact]
        public void StateStoreAndRetrieveWorksLikeACharm()
        {
            var state = new PersistentState()
            {
                CurrentTerm = 42L
            };

            _persister.Save(state);
            var persister = new LmdbPersister(_directory);
            var st2 = persister.Load();
            Assert.Equal(state.CurrentTerm, st2.CurrentTerm);
            Assert.Equal(state.Id, st2.Id);
            Assert.Equal(state.LastVotedForId, st2.LastVotedForId);
            persister.Dispose();
        }

        [Fact]
        public void AppendBadSequencingThrowsUp()
        {
            LogEntry l = new LogEntry()
            {
                Body = new byte[] { 1, 2, 3, 4 },
                Term = 42L
            };

            _persister.Append(new[] { l }, 0);
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.Append(new[] { l }, 2));
            Assert.ThrowsAny<InvalidOperationException>(() => _persister.Append(new[] { l }, 0));

            Assert.Equal(0L, _persister.LastIndex);
        }

        [Fact]
        public void CanRunSnapshotSimulationAndCantReadBeforeLogOffset()
        {
            const int BatchSize = 100;
            const long term = 42L;
            for (int i = 0; i < 1000; i++)
            {
                var entries = Enumerable.Range(i * BatchSize, BatchSize).Select(x => 
                {
                    var b = (Bufferable)Guid.NewGuid();
                    return new LogEntry()
                    {
                        Term = term,
                        Body = b.Buffer
                    };
                }).ToArray();
                _persister.Append(entries, i * BatchSize);
            }

            var lastIncludedIndex = 10_000L - 1;
            var stream = _persister.GetNextSnapshotStream(lastIncludedIndex, term);
            stream.Write(new byte[1024], 0, 1024);
            stream.Close();
            _persister.FinaliseSnapshot(lastIncludedIndex, term);
            Assert.Equal(lastIncludedIndex, _persister.LogOffset);
            Snapshot snap = null;
            Assert.True(_persister.TryGetLastSnapshot(out snap));
            Assert.Equal(lastIncludedIndex, snap.LastIncludedIndex);
            Assert.ThrowsAny<EntriesNotAvailableAnymoreException>(() => _persister.GetEntries(lastIncludedIndex, 1));
        }

        
        [Fact]
        public void CanDoSnapshotLikeAKing()
        {
            const long term = 42L;
            LogEntry l = new LogEntry()
            {
                Body = new byte[] { 1, 2, 3, 4 },
                Term = term
            };

            _persister.Append(new[] { l }, 0);
            _persister.Append(new[] { l }, 1);
            _persister.Append(new[] { l }, 2);
            _persister.Append(new[] { l }, 3);
            _persister.Append(new[] { l }, 4);
            _persister.Append(new[] { l }, 5);


            _persister.WriteLeaderSnapshot(4, term, new byte[] { 1 }, 0, false);
            _persister.WriteLeaderSnapshot(4, term, new byte[] { 2 }, 1, false);
            _persister.WriteLeaderSnapshot(4, term, new byte[] { 3 }, 2, false);
            _persister.WriteLeaderSnapshot(4, term, new byte[] { 4 }, 3, true);

            Assert.Equal(4, _persister.LogOffset);
            Snapshot snap;
            Assert.True(_persister.TryGetLastSnapshot(out snap));
            Assert.Equal(4, snap.LastIncludedIndex);
        }

        [Fact]
        public void LastIndexIsMaintained()
        {
            var position = 0;

            for (int i = 0; i < 20; i++)
            {
                var e = GetSomeEntries(variableTerm: true);
                _persister.Append(e, position);
                position += e.Length;
                Assert.Equal(position - 1, _persister.LastIndex);
                Assert.Equal(e.Last().Term, _persister.LastEntryTerm);
            }
        }

        [Fact]
        public void EntryUponEntryMaintainsAll()
        {
            var position = 0;

            for (int i = 0; i < 1000; i++)
            {
                var e = GetSomeEntries(variableTerm: true, count: 1);
                _persister.Append(e, position);
                if (i > 250)
                    TheTrace.TraceInformation("");
                TheTrace.TraceInformation("Added item at position {0}. LastIndex is {1}", position, _persister.LastIndex);
                position++;

                Assert.Equal(e.Last().Term, _persister.LastEntryTerm);
                Assert.Equal(position - 1, _persister.LastIndex);
            }
        }

        private LogEntry[] GetSomeEntries(long start = 0, int? count = null, int bufferSize = 256, long term = 42L, bool variableTerm = false)
        {
            var list = new List<LogEntry>();
            var r = new Random();
            var n = count ?? r.Next(10, 100);
            for (int i = 0; i < n; i++)
            {
                var body = new byte[bufferSize];
                r.NextBytes(body);
                if (variableTerm && r.NextDouble() < 0.1) // 10% chance
                    term++;

                list.Add(new LogEntry()
                {
                    Body = body,
                    Term = term 
                });
            }

            return list.ToArray();
        }

        public void Dispose()
        {
            TheTrace.TraceInformation("About to dispose LMDB Persister.");
            _persister.Dispose();
            TheTrace.TraceInformation("Disposed LMDB Persister.");
            
            try
            {
                Directory.Delete(_directory, true);
                TheTrace.TraceInformation("Deleted directory.");
                _writer.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}
