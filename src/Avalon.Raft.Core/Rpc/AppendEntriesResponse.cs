using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// 
    /// </summary>
    public class AppendEntriesResponse
    {
        public AppendEntriesResponse(long ownTerm, bool success, ReasonType reasonType = ReasonType.None, string reason = null)
        {
            CurrentTerm = ownTerm;
            IsSuccess = success;
            Reason = reason;
            ReasonType = ReasonType;
        }

        /// <summary>
        /// Term recognised by the follower
        /// </summary>
        public long CurrentTerm { get; }

        /// <summary>
        /// whether it was successful
        /// </summary>
        public bool IsSuccess { get; }
        
        /// <summary>
        /// Reason description in case of failure
        /// </summary>
        public string Reason { get; } 

        /// <summary>
        /// Type of the reason (in case of error)
        /// </summary>
        public ReasonType ReasonType { get; }
    }
}
