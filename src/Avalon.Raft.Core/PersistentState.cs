using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Avalon.Raft.Core
{
    public class PersistentState
    {
        const int BufferLength = 40;

        public PersistentState(Guid? seedId = null)
        {
            Id = seedId ?? Guid.NewGuid();
            CurrentTerm = 0L;
        }

        public Guid Id { get; set; }

        public virtual long CurrentTerm { get; set; }

        public virtual Guid? LastVotedForId { get; set; }

        public virtual void IncrementTerm()
        {
            CurrentTerm += 1;
            LastVotedForId = null;
        }
    }
}
