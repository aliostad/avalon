using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    public class AppendEntriesResponse
    {
        public AppendEntriesResponse(long ownTerm, bool success, string reason = null)
        {
            CurrentTerm = ownTerm;
            IsSuccess = success;
            Reason = reason;
        }

        public long CurrentTerm { get; }

        public bool IsSuccess { get; }

        public string Reason { get; } 
    }
}
