using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;

namespace Avalon.Raft.Core.Integration
{
    public class SimpleDictionaryStateMachine: IStateMachine
    {
        private ConcurrentDictionary<int, int> _state = new ConcurrentDictionary<int, int>();
        private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public Task ApplyAsync(byte[][] commands)
        {
            foreach (var command in commands)
            {
                if (command == null || command.Length != sizeof(int) * 2)
                    throw new InvalidDataException("Data expected to be two ints.");
                var key = BitConverter.ToInt32(command, 0);
                var value = BitConverter.ToInt32(command, sizeof(int));
                
                try
                {
                    _lock.EnterReadLock();
                    _state.AddOrUpdate(key, value, (k,v) => value);
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }

            return Task.CompletedTask;
        }

        public async Task RebuildFromSnapshotAsync(Snapshot snapshot)
        {
            try
            {
                _lock.EnterWriteLock();
                // make a copy
                var newFileName = Path.GetTempFileName();
                File.Copy(snapshot.FullName, newFileName, true);
                var fs = new FileStream(newFileName, FileMode.Open);
                var length = 0;
                var buffer = new byte[sizeof(int) * 2];
                _state = new ConcurrentDictionary<int, int>();
                while ((length = await fs.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    if (length != buffer.Length)
                        throw new InvalidDataException($"Could not read {buffer.Length} bytes from snapshot {snapshot.FullName}");

                    var key = BitConverter.ToInt32(buffer, 0);
                    var value = BitConverter.ToInt32(buffer, sizeof(int));
                    _state.AddOrUpdate(key, value, (k, v) => value);
                } 

                File.Delete(newFileName);
            }
            finally
            {
                _lock.ExitWriteLock();
            }

        }

        public Task WriteSnapshotAsync(Stream stream)
        {
            try
            {
                _lock.EnterWriteLock();
                foreach (var k in _state.Keys)
                {
                    stream.Write(BitConverter.GetBytes(k), 0, sizeof(int));
                    stream.Write(BitConverter.GetBytes(_state[k]), 0, sizeof(int));
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return Task.CompletedTask;
        }
    }

}
