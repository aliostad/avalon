using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public class FileStatePersister : IStatePersister
    {
        public static readonly string FileName = ".raft.state";
        private readonly string _filename;

        public FileStatePersister(string directory)
        {
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            _filename = Path.Combine(directory, FileName);
        }

        public PersistentState LoadAsync()
        {
            if (File.Exists(_filename))
                return PersistentState.FromBuffer(File.ReadAllBytes(_filename));
            else
                return null;
        }

        public void PersistAsync(PersistentState state)
        {
            File.WriteAllBytes(_filename, state.ToBytes());
        }
    }
}
