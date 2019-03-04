using Spreads.LMDB;
using Spreads;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Avalon.Common;
using Spreads.Buffers;

namespace Avalon.Raft.Core.Persistence
{
    public class LmdbPersister : ILogPersister, IStatePersister, IDisposable
    {
        private const long LogKey = 0L;

        private readonly string _directory;
        private readonly LMDBEnvironment _env;
        private readonly Configuration _config;
        private readonly object _lock = new object();
        private PersistentState _state = null;

        public class StateDbKeys
        {
            public static readonly string LogOffset = "LogOffset";
            public static readonly string Id = "Id";
            public static readonly string CurrentTerm = "CurrentTerm";
            public static readonly string LastVotedFor = "LastVotedFor";
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
            _env.Open();
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
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                using (var ldb = OpenLogDatabase())
                {
                    var c = ldb.OpenReadOnlyCursor(tx);
                    var k = LogKey + 1; // to move to last position
                    long value;
                    if (c.TryFind(Lookup.EQ, ref k, out value))
                    {
                        this.LastIndex = value;
                    }
                }

                using (var sdb = OpenStateDatabase())
                {
                    Bufferable value = default;
                    if (tx.TryGet(sdb, StateDbKeys.LogOffset, out value))
                    {
                        LogOffset = value;
                    }

                    if (tx.TryGet(sdb, StateDbKeys.Id, out value))
                    {
                        _state = new PersistentState();
                        _state.Id = value;

                        if (tx.TryGet(sdb, StateDbKeys.CurrentTerm, out value)) // this should be always TRUE
                            _state.CurrentTerm = value;

                        if (tx.TryGet(sdb, StateDbKeys.LastVotedFor, out value))
                            _state.LastVotedForId = value;
                    }
                }
            }
        }

        public long LogOffset { get; set; } = 0;

        public long LastIndex { get; set; } = -1;

        public void Append(LogEntry[] entries, long startingOffset)
        {
            lock(_lock)
            {
                if (startingOffset != LastIndex + 1)
                    throw new InvalidOperationException($"Starting index is {startingOffset} but LastIndex is {LastIndex}");

                using (var tx = _env.BeginTransaction())
                using (var db = OpenLogDatabase())
                {
                    foreach (var e in entries.Zip(Enumerable.Range(0, entries.Length), (l, i) => (i, l)).Select(x => ((Bufferable)x.l).PrefixWithIndex(x.i)))
                    {
                        tx.Put(db, LogKey, e, TransactionPutOptions.AppendDuplicateData);
                    }

                    tx.Commit();

                    this.LastIndex += entries.Length;
                }
            }
        }

        public void DeleteEntries(long fromIndex)
        {
            lock (_lock)
            {
                using (var tx = _env.BeginTransaction())
                using (var db = OpenLogDatabase())
                using (var c = db.OpenCursor(tx))
                {
                    long key = LogKey;
                    int i = 0;
                    if (!c.TryFindDup(Lookup.EQ, ref key, ref fromIndex))
                    {
                        throw new InvalidOperationException($"Could not find index {fromIndex}");
                    }

                    while (c.Delete())
                        i++;

                    tx.Commit();
                }
            }
        }

        public LogEntry[] GetEntries(long index, int count)
        {
            if (index + count < LastIndex)
                throw new InvalidOperationException($"We do not have these entries. index: {index}, count: {count} and LastIndex: {LastIndex}");

            var list = new LogEntry[count];
            using (var tx = _env.BeginReadOnlyTransaction())
            using (var db = OpenStateDatabase())
            {
                for (long i = index; i < index + count; i++)
                {
                    Bufferable b = i;
                    if (!tx.TryGetDuplicate(db, LogKey, ref b))
                        throw new InvalidOperationException($"Could not find index {i} in the logs.");
                    StoredLogEntry s = b.Buffer;
                    if (s.Index != index)
                        throw new InvalidDataException($"Corruption in the highest. Supposedly loaded {index} but came out {s.Index}");

                    list[i - index] = s.Body;
                }
            }

            return list;
        }

        public void WriteSnapshot(long lastIncludedIndex, byte[] chunk, long offsetInFile, bool isFinal)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _env.Close();
        }

        public void Save(PersistentState state)
        {
            lock (_lock) // TODO: unnecessary probably
            {
                using (var tx = _env.BeginTransaction())
                using (var db = OpenStateDatabase())
                {
                    if (state.LastVotedForId.HasValue)
                        tx.Put(db, StateDbKeys.LastVotedFor, state.LastVotedForId.Value);
                    tx.Put(db, StateDbKeys.Id, state.Id);
                    tx.Put(db, StateDbKeys.CurrentTerm, state.CurrentTerm);
                    tx.Commit();
                    _state = state;
                }
            }
        }

        public PersistentState Load()
        {
            return _state;
        }

        public void SaveLastVotedFor(Guid id)
        {
            lock (_lock) // TODO: unnecessary probably
        {
                using (var tx = _env.BeginTransaction())
                using (var db = OpenStateDatabase())
                {
                    tx.Put(db, StateDbKeys.LastVotedFor, id);
                    tx.Commit();
                    _state.LastVotedForId = id;
                }
            }
        }
    }
}
