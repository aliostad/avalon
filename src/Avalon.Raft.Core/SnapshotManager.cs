using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// Manages snapshots
    /// </summary>
    internal class SnapshotManager
    {
        private readonly string _directory;

        public SnapshotManager(string directory)
        {
            _directory = directory;
            if (!Directory.Exists(_directory))
                Directory.CreateDirectory(_directory);
        }

        public string GetTempFileNameForIndexAndTerm(long index, long term)
        {
            return Path.Combine(_directory, $"{index}_{term}.snapshot.tmp");
        }

        public string GetFinalFileNameForIndexAndTerm(long index, long term)
        {
            return Path.Combine(_directory, $"{index}_{term}.snapshot");
        }

        internal Snapshot GetSnapshot(string fileName)
        {
            var indexAndTerm = Path.GetFileName(fileName).Split('.')[0].Split('_');
            var index = Convert.ToInt64(indexAndTerm[0]);
            var term = Convert.ToInt64(indexAndTerm[1]);
            return new Snapshot(){
                FullName = fileName,
                LastIncludedIndex = index,
                LastIncludedTerm = term
            };
        }

        private bool IsTemp(string fileName)
        {
            return fileName.EndsWith(".tmp");
        }

        public Snapshot GetLastSnapshot()
        {
            Snapshot snap = null;
            foreach (var f in Directory.GetFiles(_directory, "*.snapshot"))
            {
                if(!IsTemp(f))
                {
                    var ss = GetSnapshot(f);
                    if (ss == null || snap == null || snap.LastIncludedIndex < ss.LastIncludedIndex)
                    {
                        snap = ss;
                    }
                }
            }

            return snap;
        }

        public IEnumerable<string> GetPreviousSnapshots()
        {
            var list = new List<string>();
            var snap = GetLastSnapshot();

            foreach (var f in Directory.GetFiles(_directory, "*.snapshot*"))
            {
                var ss = GetSnapshot(f);
                if (snap == null || ss.LastIncludedIndex < snap.LastIncludedIndex)
                    list.Add(f);
            }

            return list;
        }
    }
}
