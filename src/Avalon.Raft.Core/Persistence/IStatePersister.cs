using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    public interface IStatePersister
    {
        void Save(PersistentState state);

        void SaveLastVotedFor(Guid id);

        PersistentState Load();


    }
}
