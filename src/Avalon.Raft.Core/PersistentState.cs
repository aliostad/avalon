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
            CurrentTerm = 0L;
        }

        public Guid Id { get; set; }

        public long CurrentTerm { get; set; }

        public Guid? LastVotedForId { get; set; }

        public void IncrementTerm()
        {
            CurrentTerm += 1;
            LastVotedForId = null;
        }
    }
}
