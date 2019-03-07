using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// Manages files where the name represents an index (number)
    /// </summary>
    public class IndexedFileManager
    {
        private readonly string _directory;



        public IndexedFileManager(string directory)
        {
            _directory = directory;
            if (!Directory.Exists(_directory))
                Directory.CreateDirectory(_directory);
        }

        public string GetTempFileNameForIndex(long index)
        {
            return Path.Combine(_directory, $"{index}.snapshot.tmp");
        }

        public string GetFinalFileNameForIndex(long index)
        {
            return Path.Combine(_directory, $"{index}.snapshot");
        }

        internal long GetSnapshotIndex(string fileName)
        {
            return Convert.ToInt64(Path.GetFileName(fileName).Split('.')[0]);
        }

        private bool IsTemp(string fileName)
        {
            return fileName.EndsWith(".tmp");
        }

        public long? GetLastFinalIndex()
        {
            long? max = null;
            foreach (var f in Directory.GetFiles(_directory, "*.snapshot"))
            {
                if(!IsTemp(f))
                {
                    var index = GetSnapshotIndex(f);
                    if (!max.HasValue || max.Value < index)
                        max = index;
                }
            }

            return max;
        }

        public IEnumerable<string> GetPreviousSnapshots()
        {
            var list = new List<string>();
            var max = GetLastFinalIndex() ?? -1;

            foreach (var f in Directory.GetFiles(_directory, "*.snapshot*"))
            {
                var index = GetSnapshotIndex(f);
                if (index < max)
                    list.Add(f);
            }

            return list;
        }
    }
}
