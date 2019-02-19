using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public class FileLogPersister : ILogPersister
    {
        private readonly string _directory;
        private long _offset;

        private const string OffsetFileName = ".offset";

        public FileLogPersister(string directory)
        {
            _directory = directory;
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

        public int FirstIndexOffset => throw new NotImplementedException();

        public Task AppendAsync(LogEntry entry)
        {
            throw new NotImplementedException();
        }

        public Task CompactAsync(int fromIndex)
        {
            File.
            throw new NotImplementedException();
        }

        public Task DeleteEntriesAsync(int fromIndex)
        {
            throw new NotImplementedException();
        }

        public Task<LogEntry[]> GetEntriesAsync(int index, int count)
        {
            throw new NotImplementedException();
        }
    }
}
