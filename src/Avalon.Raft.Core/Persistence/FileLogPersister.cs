using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public class FileLogPersister : ILogPersister
    {
        private const string OffsetFileName = ".avalon.raft.offset";
        private const string LogFileName = ".avalon.raft.log";
        private const string SnapshotFileName = ".avalon.raft.snapshot";

        private readonly string _directory;
        private long _offset;
        private readonly int _sizeOfLogEntry;
        private FileStream _logFile;
        private readonly SequentialFileNameManager _seqman;


        public FileLogPersister(string directory, int sizeOfLogEntry)
        {
            _directory = directory;
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            _seqman = new SequentialFileNameManager(directory, LogFileName);
            _sizeOfLogEntry = sizeOfLogEntry;
            LoadOffset();
            _logFile = new FileStream(_seqman.CurrentFullFileName, mode: FileMode.OpenOrCreate);
        }

        private void LoadOffset()
        {
            var name = Path.Combine(_directory, OffsetFileName);
            if (File.Exists(name))
            {
                _offset = BitConverter.ToInt64(File.ReadAllBytes(name), 0);
            }
        }

        private void SaveOffset()
        {
            var name = Path.Combine(_directory, OffsetFileName);
            File.WriteAllBytes(name, BitConverter.GetBytes(_offset));
        }

        public long LogOffset => _offset;

        public long LastIndex => (_logFile.Length / _sizeOfLogEntry) + _offset;

        public Task AppendAsync(LogEntry entry)
        {
            return _logFile.WriteAsync(entry, 0, _sizeOfLogEntry, CancellationToken.None);
        }

        public Task CompactAsync(long fromIndex)
        {
            throw new NotImplementedException();
        }

        public Task DeleteEntriesAsync(long fromIndex)
        {
            throw new NotImplementedException();
        }

        public Task<LogEntry[]> GetEntriesAsync(long index, int count)
        {
            throw new NotImplementedException();
        }
    }
}
