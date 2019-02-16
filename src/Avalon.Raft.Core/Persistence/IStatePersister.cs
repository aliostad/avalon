using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Persistence
{
    public interface IStatePersister
    {
        void PersistAsync(PersistentState state);

        PersistentState LoadAsync();
    }
}
