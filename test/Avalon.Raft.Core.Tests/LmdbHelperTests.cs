using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Spreads.LMDB;
using Xunit;
using Avalon.Common;
using System.Diagnostics;

namespace Avalon.Raft.Core.Tests
{
    public class LmdbHelperTests : IDisposable
    {
        private readonly string _directory;
        protected LMDBEnvironment _env;
        protected const string DatabaseName = "simit";
        private StreamWriter _writer;
        private readonly object _lock = new object();
        private readonly string _correlationId = Guid.NewGuid().ToString("N");

        private const bool OutputTraceLog = true;

        public LmdbHelperTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _env = LMDBEnvironment.Create(_directory, disableAsync: true);
            _env.MapSize = 100 * 1024 * 1024;
            _env.Open();

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
                        _writer.WriteLine(message);
                    }
                };
            }
        }

        [Fact]
        public void CanSaveAndReadString()
        {
            string key = "jam!";
            string value = "jim!";

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, b);
                }
            }
        }

        [Fact]
        public void CanSaveAndReadLong()
        {
            var key = 42L;
            var value = 1969L;

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, (long) b);
                }
            }
        }

        [Fact]
        public void CanSaveAndReadintguid()
        {
            var key = 42;
            var value = Guid.NewGuid();

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create)))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, value);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = default;
                    var success = tx.TryGet(db, key, out b);
                    Assert.True(success);
                    Assert.Equal(value, (Guid) b);
                }
            }
        }

        [Fact]
        public void CanSaveAndReadDup()
        {
            var key = 42;
            var value = Guid.NewGuid();
            var i = 1969L;
            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort) { DupSortPrefix = 64 }))
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(db, key, new Bufferable(i, value), TransactionPutOptions.AppendDuplicateData);
                    tx.Put(db, key, new Bufferable(i + 1, value), TransactionPutOptions.AppendDuplicateData);
                    tx.Put(db, key, new Bufferable(i + 2, value), TransactionPutOptions.AppendDuplicateData);
                    tx.Put(db, key, new Bufferable(i + 3, value), TransactionPutOptions.AppendDuplicateData);
                    tx.Commit();
                }

                using (var tx = _env.BeginReadOnlyTransaction())
                {
                    Bufferable b = new Bufferable(i + 2);

                    var success = tx.TryGetDuplicate(db, key, ref b);
                    Assert.True(success);
                    Assert.Equal(i + 2, BitConverter.ToInt64(b.Buffer, 0));
                }
            }
        }

        [Fact]
        public void CanDeleteUpTo()
        {
            var key = 42L;
            var value = Guid.NewGuid();
            var total = 1000_000L;
            var upto = 100_000L;

            using (var db = _env.OpenDatabase(DatabaseName, new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort) { DupSortPrefix = 64 }))
            {
                using (var tx = _env.BeginTransaction())
                {
                    for (long i = 0; i < total; i++)
                    {
                        tx.Put(db, key, new Bufferable(i, value), TransactionPutOptions.AppendDuplicateData);
                    }
                   
                    tx.Commit();
                }

                Assert.Equal(total, db.GetEntriesCount());

                using (var tx2 = _env.BeginTransaction())
                {
                    Assert.True(tx2.DeleteUpToValue(db, key, upto));
                    tx2.Commit();
                }

                Assert.Equal(total - upto, db.GetEntriesCount());

                using (var tx3 = _env.BeginReadOnlyTransaction())
                {
                    Bufferable checkValue = 1000L;
                    tx3.TryGetDuplicate(db, key, ref checkValue);

                    checkValue = 100_000L;
                    Assert.True(tx3.TryGetDuplicate(db, key, ref checkValue));
                }

            }
        }

        public void Dispose()
        {
            try
            {
                _env.Dispose();
                Directory.Delete(_directory, true);
            }
            catch (Exception e)
            {
                Trace.TraceWarning(e.ToString());
            }
        }
    }
}
