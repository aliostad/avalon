using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    public class InstallUpdateResponse
    {
        /// <summary>
        /// currentTerm, for leader to update itself
        /// </summary>
        public long CurrentTerm { get; set; }
    }
}
