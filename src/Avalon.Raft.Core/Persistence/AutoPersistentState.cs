using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    public class AutoPersistentState : PersistentState
    {
        private readonly IStatePersister _persister;

        public AutoPersistentState(IStatePersister persister)
        {
            if (persister == null)
                throw new ArgumentNullException("persister");
                
            _persister = persister;
            var state = persister.Load();
            base.Id = state.Id;
            base.CurrentTerm = state.CurrentTerm;
            base.LastVotedForId = state.LastVotedForId;
        }

        public override void IncrementTerm()
        {
            base.IncrementTerm();
        }

        public override long CurrentTerm
        {
            get => base.CurrentTerm;
            set
            {
                base.CurrentTerm = value;
                _persister.SaveTerm(value);
            }
        }

        public override Guid? LastVotedForId
        {
            get => base.LastVotedForId;
            set
            {
                base.LastVotedForId = value;
                _persister.SaveLastVotedFor(value);
            }
        }
    }
}
