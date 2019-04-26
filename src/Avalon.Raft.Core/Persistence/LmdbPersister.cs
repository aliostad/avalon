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
    /// <summary>
    /// Implements various persistance requirements using LMDB (and files for snapshot)
    /// 
    /// This is the LogOffset, LastIncludedIndex
    /// snapshot => || 0 | 1 | 2 | 3 ||
    /// and logs => || 4 | 5 | 6 | 7 | 8 | ...
    ///     LastIncludedIndex   -> 3
    ///     LogOffset           -> 4
    /// 
    /// </summary>
    public class LmdbPersister : ILogPersister, IStatePersister, ISnapshotOperator
    {
        private const long LogKey = 0L;

        private readonly string _directory;
        private readonly LMDBEnvironment _env;
        private readonly object _lock = new object();
        private PersistentState _state = null;
        private readonly Database _logDb;
        private readonly Database _stateDb;
        private readonly SnapshotManager _snapMgr;

        public class StateDbKeys
        {
            public static readonly string Id = "Id";
            public static readonly string CurrentTerm = "CurrentTerm";
            public static readonly string LastVotedFor = "LastVotedFor";
        }

        class Databases
        {
            public static readonly string Log = "avalon_log";
            public static readonly string State = "avalon_state";
        }

        /// <summary>
        /// Creates a new
        /// </summary>
        /// <param name="directory">directory where the data and snapshots kept</param>
        /// <param name="mapSize">Default is 100 MB</param>
        public LmdbPersister(string directory, long mapSize = 100 * 1024 * 1024, Guid? seedId = null)
        {
            _directory = directory;
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            _env = LMDBEnvironment.Create(directory, disableAsync: true);
            _env.MapSize = mapSize;
            _env.Open();

            _snapMgr = new SnapshotManager(_directory);

            _logDb = _env.OpenDatabase(Databases.Log, new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort) { DupSortPrefix = 64 });
            _stateDb = _env.OpenDatabase(Databases.State, new DatabaseConfig(DbFlags.Create));

            LoadState(seedId);
        }

        private void LoadLastTermAndIndex()
        {
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                LoadLastTermAndIndex(tx);
            }
        }

        private void LoadLastTermAndIndex(ReadOnlyTransaction tx)
        {
            using (var c = _logDb.OpenReadOnlyCursor(tx))
            {
                var k = LogKey;
                StoredLogEntryHeader value = new StoredLogEntryHeader()
                {
                    Index = long.MaxValue
                };

                if (c.TryFindDup(Lookup.LE, ref k, ref value))
                {
                    this.LastIndex = value.Index;
                    this.LastEntryTerm = value.Term;
                }
            }
        }

        private void LoadState(Guid? seedId = null)
        {
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                LoadLastTermAndIndex(tx);
                
                // snapshot
                Snapshot ss;
                if (TryGetLastSnapshot(out ss))
                    LogOffset = ss.LastIncludedIndex + 1;
                
                // state
                _state = new PersistentState(seedId);
                Bufferable val;
                if (tx.TryGet(_stateDb, StateDbKeys.Id, out val))
                {
                    _state.Id = val;

                    if (tx.TryGet(_stateDb, StateDbKeys.CurrentTerm, out val)) // this should be always TRUE
                        _state.CurrentTerm = val;

                    if (tx.TryGet(_stateDb, StateDbKeys.LastVotedFor, out val))
                    {
                        Guid g = val;
                        _state.LastVotedForId = g == Guid.Empty ? (Guid?) null : g;
                    }
                }
            }

            TheTrace.TraceInformation($"Seed Id was {seedId} and now {_state.Id}");
        }

        /// <inheritdocs/>
        public long LogOffset { get; private set; } = 0;

        /// <inheritdocs/>
        public long LastIndex { get; private set; } = -1;

        /// <inheritdocs/>
        public long LastEntryTerm { get; private set; } = -1;

        /// <inheritdocs/>
        public void Append(LogEntry[] entries, long? startingOffset = null)
        {
            if (startingOffset.HasValue && startingOffset.Value != LastIndex + 1)
                throw new InvalidOperationException($"Starting index is {startingOffset} but LastIndex is {LastIndex}");
            
            if (entries.Length == 0)
                throw new InvalidOperationException("Entries is empty.");

            lock (_lock)
            {
                startingOffset = LastIndex + 1;
                var indices = Enumerable.Range(0, entries.Length).Select(x => x + startingOffset);
                using (var tx = _env.BeginTransaction())
                {
                    foreach (var e in entries.Zip(indices, (l, i) => (i, l)).Select(x => new Bufferable(x.l.Body).PrefixWithIndexAndTerm(x.i.Value, x.l.Term)))
                    {
                        tx.Put(_logDb, LogKey, e, TransactionPutOptions.AppendDuplicateData);
                    }

                    tx.Commit();
                }

                LoadLastTermAndIndex();
                if (LastIndex != startingOffset + entries.Length - 1)
                {
                    throw new InvalidOperationException($"THIS IS BAD!! expected {startingOffset} + {entries.Length} but found {LastIndex}.");
                }
                if (LastEntryTerm != entries.Last().Term)
                {
                    throw new InvalidOperationException($"THIS IS BAD!! expected last term {entries.Last().Term} but found {LastEntryTerm}.");
                }

            }
        }

        /// <inheritdocs/>
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
                }

                LoadLastTermAndIndex();
            }
        }

        /// <inheritdocs/>
        public LogEntry[] GetEntries(long index, int count)
        {
            if (index < LogOffset)
                throw new EntriesNotAvailableAnymoreException(index, LogOffset);

            if (index + count - 1 > LastIndex)
                throw new InvalidOperationException($"We do not have these entries yet. index: {index}, count: {count} and LastIndex: {LastIndex}");

            var list = new LogEntry[count];
            using (var tx = _env.BeginReadOnlyTransaction())
            {
                for (long i = index; i < index + count; i++)
                {
                    Bufferable b = i;
                    if (!tx.TryGetDuplicate(_logDb, LogKey, ref b))
                        throw new InvalidOperationException($"Could not find index {i} in the logs.");
                    StoredLogEntry s = b.Buffer;
                    if (s.Index != i)
                        throw new InvalidDataException($"Corruption in the highest. Supposedly loaded {index} but came out {s.Index}");

                    list[i - index] = new LogEntry()
                    {
                        Body = s.Body,
                        Term = s.Term
                    };
                }
            }

            return list;
        }

        /// <inheritdocs/>
        public void WriteLeaderSnapshot(long lastIncludedIndex, long lastTerm, byte[] chunk, long offsetInFile, bool isFinal)
        {
            var fileName = _snapMgr.GetTempFileNameForIndexAndTerm(lastIncludedIndex, lastTerm);
            Stream stream = null;
            if (File.Exists(fileName))
            {
                var info = new FileInfo(fileName);
                if (offsetInFile != info.Length)
                    throw new InvalidOperationException($"Bad position. Snapshot chunk is at {offsetInFile} but file has a size of {info.Length}.");
                stream = new FileStream(fileName, FileMode.Open);
                stream.Position = info.Length;
            }
            else
            {
                if (offsetInFile != 0)
                    throw new InvalidOperationException($"Bad position. Snapshot chunk is at {offsetInFile} but file has a size of zero.");

                stream = new FileStream(fileName, FileMode.OpenOrCreate);
            }

            stream.Write(chunk, 0, chunk.Length);
            stream.Close();

            if (isFinal)
            {
                var newOffset = lastIncludedIndex + 1;
                FinaliseSnapshot(lastIncludedIndex, lastTerm);
                TruncateLogUpToIndex(newOffset);
            }
        }

        private void TruncateLogUpToIndex(long index)
        {
            using (var tx = _env.BeginTransaction())
            {
                tx.DeleteUpToValue(_logDb, LogKey, index);
            }
        }

        /// <inheritdocs/>
        public void Dispose()
        {
            _stateDb.Dispose();
            _logDb.Dispose();
            _env.Dispose();
        }

        /// <inheritdocs/>
        public void Save(PersistentState state)
        {
            lock (_state) // TODO: unnecessary probably
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(_stateDb, StateDbKeys.LastVotedFor, state.LastVotedForId ?? Guid.Empty);
                    tx.Put(_stateDb, StateDbKeys.Id, state.Id);
                    tx.Put(_stateDb, StateDbKeys.CurrentTerm, state.CurrentTerm);
                    tx.Commit();
                    _state = state;
                }
            }
        }

        /// <inheritdocs/>
        public PersistentState Load()
        {
            return _state;
        }

        /// <inheritdocs/>
        public void SaveLastVotedFor(Guid? id)
        {
            lock (_state)
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(_stateDb, StateDbKeys.LastVotedFor, id ?? Guid.Empty);
                    tx.Commit();
                    _state.LastVotedForId = id;
                }
            }
        }

        /// <inheritdocs/>
        public void CleanSnapshots()
        {
            try
            {
                foreach (var f in _snapMgr.GetPreviousSnapshots())
                    File.Delete(f);
            }
            catch (Exception e)
            {
                TheTrace.TraceWarning(e.ToString());
            }

        }
        
        /// <inheritdocs/>
        public Stream GetNextSnapshotStream(long lastIndexIncluded, long lastTerm)
        {
            if (lastIndexIncluded < LogOffset)
                throw new InvalidOperationException($"lastIndexIncluded of {lastIndexIncluded} is less than LogOffset of {LogOffset}");

            if (lastIndexIncluded > LastIndex)
                throw new InvalidOperationException($"lastIndexIncluded of {lastIndexIncluded} is greater than LastIndex of {LastIndex}");

            return new FileStream(_snapMgr.GetTempFileNameForIndexAndTerm(lastIndexIncluded, lastTerm), FileMode.OpenOrCreate);
        }

        /// <inheritdocs/>
        public void FinaliseSnapshot(long lastIndexIncluded, long lastTerm)
        {
            File.Move(
                _snapMgr.GetTempFileNameForIndexAndTerm(lastIndexIncluded, lastTerm), 
                _snapMgr.GetFinalFileNameForIndexAndTerm(lastIndexIncluded, lastTerm));
            this.LogOffset = lastIndexIncluded + 1; // this is it! if server goes down, it will find LogOffset from file names
        }

        /// <inheritdocs/>
        public bool TryGetLastSnapshot(out Snapshot snapshot)
        {
            snapshot = _snapMgr.GetLastSnapshot();
            return snapshot != null;
        }

        /// <inheritdocs/>
        public void SaveTerm(long newTerm)
        {
            lock (_state)
            {
                using (var tx = _env.BeginTransaction())
                {
                    tx.Put(_stateDb, StateDbKeys.CurrentTerm, newTerm);
                    tx.Commit();
                    _state.CurrentTerm = newTerm;
                }
            }
        }

        /// <inheritdocs/>
        public void ApplySnapshot(long newFirstIndex)
        {
            // this MUST be called after snapshot is finalised hence it checks if they are the same
            if (newFirstIndex != LogOffset)
                throw new InvalidOperationException($"Expecting newFirstIndex to be as LogOffset+1 ({LogOffset}+1) but was {newFirstIndex}");
            TruncateLogUpToIndex(newFirstIndex);
        }
    }
}
