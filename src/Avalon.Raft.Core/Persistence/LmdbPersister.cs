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
        private readonly object _lock = new object();
        private PersistentState _state = null;
        private readonly Database _logDb;
        private readonly Database _stateDb;


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

        public LmdbPersister(string directory, long mapSize = 100 * 1024 * 1024)
        {
            _directory = directory;
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            _env = LMDBEnvironment.Create(directory);
            _env.MapSize = mapSize;

            _env.Open();
            _logDb = _env.OpenDatabase(Databases.Log, new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates));
            _stateDb = _env.OpenDatabase(Databases.State, new DatabaseConfig(DbFlags.Create));

            LoadState();
        }

        private void LoadState()
        {
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                
                var c = _logDb.OpenReadOnlyCursor(tx);
                var k = LogKey + 1; // to move to last position
                long value;
                if (c.TryFind(Lookup.EQ, ref k, out value))
                {
                    this.LastIndex = value;
                }

               
                Bufferable val = default;
                if (tx.TryGet(_stateDb, StateDbKeys.LogOffset, out val))
                {
                    LogOffset = value;
                }

                if (tx.TryGet(_stateDb, StateDbKeys.Id, out val))
                {
                    _state = new PersistentState();
                    _state.Id = val;

                    if (tx.TryGet(_stateDb, StateDbKeys.CurrentTerm, out val)) // this should be always TRUE
                        _state.CurrentTerm = val;

                    if (tx.TryGet(_stateDb, StateDbKeys.LastVotedFor, out val))
                        _state.LastVotedForId = val;
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
                var indices = Enumerable.Range(0, entries.Length).Select(x => x + startingOffset);
                using (var tx = _env.BeginTransaction())
                {
                    foreach (var e in entries.Zip(indices, (l, i) => (i, l)).Select(x => ((Bufferable)x.l).PrefixWithIndex(x.i)))
                    {
                        tx.Put(_logDb, LogKey, e, TransactionPutOptions.AppendDuplicateData);
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
                using (var c = _logDb.OpenCursor(tx))
                {
                    long key = LogKey;
                    int i = 0;
                    if (!c.TryFindDup(Lookup.EQ, ref key, ref fromIndex))
                    {
                        throw new InvalidOperationException($"Could not find index {fromIndex}");
                    }

                    while (c.Delete(false))
                        i++;

                    tx.Commit();
                    LastIndex = fromIndex - 1;
                }
            }
        }

        public LogEntry[] GetEntries(long index, int count)
        {
            if (index + count < LastIndex)
                throw new InvalidOperationException($"We do not have these entries. index: {index}, count: {count} and LastIndex: {LastIndex}");

            var list = new LogEntry[count];
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                for (long i = index; i < index + count; i++)
                {
                    Bufferable b = i;
                    if (!tx.TryGetDuplicate(_logDb, LogKey, ref b))
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
            _stateDb.Dispose();
            _logDb.Dispose();
            _env.Close();
        }

        public void Save(PersistentState state)
        {
            lock (_lock) // TODO: unnecessary probably
            {
                using (var tx = _env.BeginTransaction())
                {
                    if (state.LastVotedForId.HasValue)
                        tx.Put(_stateDb, StateDbKeys.LastVotedFor, state.LastVotedForId.Value);
                    tx.Put(_stateDb, StateDbKeys.Id, state.Id);
                    tx.Put(_stateDb, StateDbKeys.CurrentTerm, state.CurrentTerm);
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
                {
                    tx.Put(_stateDb, StateDbKeys.LastVotedFor, id);
                    tx.Commit();
                    _state.LastVotedForId = id;
                }
            }
        }
    }
}
