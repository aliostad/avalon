using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    public class AppendEntriesResponse
    {
        public long CurrentTerm { get; set; }

        public bool IsSuccess { get; set; }
    }
}
