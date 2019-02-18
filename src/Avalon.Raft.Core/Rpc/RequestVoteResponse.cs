using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// 
    /// </summary>
    public class RequestVoteResponse
    {
        /// <summary>
        /// currentTerm, for candidate to update itself
        /// </summary>
        public long CurrentTrem { get; set; }

        /// <summary>
        /// true means candidate received vote
        /// </summary>
        public bool VoteGranted { get; set; }
    }
}
