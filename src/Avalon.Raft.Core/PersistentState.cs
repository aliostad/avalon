using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Avalon.Raft.Core
{
    public class PersistentState
    {
        const int BufferLength = 40;

        public PersistentState()
        {
            Id = Guid.NewGuid();
            LastVotedForId = new Guid();
            CurrentTerm = 0L;
        }

        public Guid Id { get; set; }

        public long CurrentTerm { get; set; }

        public Guid LastVotedForId { get; set; }

        public byte[] ToBytes()
        {
            var bb = new List<byte>();
            bb.AddRange(Id.ToByteArray());
            bb.AddRange(BitConverter.GetBytes(CurrentTerm));
            bb.AddRange(LastVotedForId.ToByteArray());
            return bb.ToArray();
        }

        public static PersistentState FromBuffer(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException($"{nameof(buffer)}");

            if (buffer.Length != 40)
                throw new InvalidOperationException($"Buffer must be {BufferLength} but was {buffer.Length}");

            return new PersistentState()
            {
                Id = new Guid(buffer.Take(16).ToArray()),
                CurrentTerm = BitConverter.ToInt64(buffer, 16),
                LastVotedForId = new Guid(buffer.Skip(16+8).ToArray())
            };
        }
    }
}
