using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// 
    /// </summary>
    public class RequestVoteRequest
    {
        /// <summary>
        /// candidate’s term
        /// </summary>
        public long CurrentTerm { get; set; }

        /// <summary>
        /// candidate requesting vote
        /// </summary>
        public Guid CandidateId { get; set; }

        /// <summary>
        /// index of candidate’s last log entry
        /// </summary>
        public long LastLogIndex { get; set; }

        /// <summary>
        /// term of candidate’s last log entry
        /// </summary>
        public long LastLogTerm { get; set; }
    }
}
