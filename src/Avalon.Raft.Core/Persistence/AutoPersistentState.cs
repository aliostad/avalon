using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    public class AutoPersistentState : PersistentState
    {
        private readonly IStatePersister _persister;
        private bool _initialLocading = true;

        public AutoPersistentState(IStatePersister persister)
        {
            _persister = persister;
            var state = persister.Load();

            _initialLocading = false;
        }

        public override void IncrementTerm()
        {
            base.IncrementTerm();
            _persister.SaveTerm(this.CurrentTerm);
        }

        public override long CurrentTerm
        {
            get => base.CurrentTerm;
            set
            {
                base.CurrentTerm = value;
                if (!_initialLocading)
                    _persister.SaveTerm(value);
            }
        }

        public override Guid? LastVotedForId
        {
            get => base.LastVotedForId;
            set
            {
                if (!value.HasValue)
                    throw new ArgumentNullException("Y U NO VALUE??! YOU CANNOT SET TO NULL.");

                base.LastVotedForId = value;
                if (!_initialLocading)
                    _persister.SaveLastVotedFor(value.Value);
            }
        }
    }
}
