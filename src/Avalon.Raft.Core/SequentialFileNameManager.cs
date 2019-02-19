using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// Creates and manages filenames in the format of "<directory>\<filename>.n" where n is the sequence
    /// Starting at zero
    /// </summary>
    public class SequentialFileNameManager
    {
        private int _currentSequence = 0;
        private readonly string _fullBaseName;
        private readonly string _directory;
        private readonly string _baseFileName;

        public SequentialFileNameManager(string directory, string baseFileName)
        {
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            _directory = directory;
            _baseFileName = baseFileName;

            foreach(var f in Directory.GetFiles(directory, baseFileName + ".*"))
            {
                var index = Convert.ToInt32(Path.GetFileName(f).Replace(baseFileName + ".", ""));
                if (index > _currentSequence)
                    _currentSequence = index;
            }

            _fullBaseName = Path.Combine(directory, baseFileName);
        }

        public string CurrentFullFileName => _fullBaseName + "." + CurrentSequence;

        public string NextFullFileName => _fullBaseName + "." + (CurrentSequence + 1);

        public int Increment()
        {
            return ++_currentSequence;
        }

        /// <summary>
        /// Clears the folder and only keeps the last item
        /// </summary>
        public void Clear()
        {
            foreach (var f in Directory.GetFiles(_directory, _baseFileName + ".*"))
            {
                if (f != CurrentFullFileName)
                    File.Delete(f);
            }
        }

        public int CurrentSequence => _currentSequence;
    }
}
