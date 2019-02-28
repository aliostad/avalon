using Spreads.LMDB;
using Spreads;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    public class LmdbPersister : ILogPersister, IDisposable
    {

        private const long LogKey = 0L;

        private readonly string _directory;
        private readonly LMDBEnvironment _env;
        private readonly Configuration _config;
        private readonly object _lock = new object();

        public class StateDbKeys
        {
            public static readonly long LogOffset = 0L;
        }

        public class Databases
        {
            public static readonly string Log = "avalon_log";
            public static readonly string State = "avalog_state";
        }

        public LmdbPersister(string directory, Configuration config)
        {
            _directory = directory;
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            _env = LMDBEnvironment.Create(directory);
            _config = config;
            
            LoadState();
        }

        private Database OpenLogDatabase()
        {
            return _env.OpenDatabase(Databases.Log, new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
        }

        private Database OpenStateDatabase()
        {
            return _env.OpenDatabase(Databases.State, new DatabaseConfig(DbFlags.Create));
        }

        private void LoadState()
        {
            using (var tx = _env.BeginTransaction())
            {
                using (var ldb = OpenLogDatabase())
                {
                    var c = ldb.OpenCursor(tx);
                    var k = LogKey + 1; // to move to last position
                    long value;
                    if (c.TryFind(Lookup.EQ, ref k, out value))
                    {
                        this.LastIndex = value;
                    }
                }

                using (var sdb = OpenStateDatabase())
                {
                    //sdb.get
                }
            }
        }

        public long LogOffset { get; set; }

        public long LastIndex { get; set; } = -1;

        public void Append(LogEntry[] entries, long startingOffset)
        {
            if (startingOffset != LastIndex + 1)
                throw new InvalidOperationException($"Starting index is {startingOffset} but LastIndex is {LastIndex}");
        }

        public void DeleteEntries(long fromIndex)
        {
            throw new NotImplementedException();
        }

        public LogEntry[] GetEntries(long index, int count)
        {
            throw new NotImplementedException();
        }

        public void WriteSnapshot(long lastIncludedIndex, byte[] chunk, long offsetInFile, bool isFinal)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _env.Dispose();
        }
    }
}
