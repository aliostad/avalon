using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    public enum ReasonType
    {
        /// <summary>
        /// Default
        /// </summary>
        None,

        /// <summary>
        /// Update failed because of log inconsistency
        /// </summary>
        LogInconsistency,

        /// <summary>
        /// Leader's term not right
        /// </summary>
        TermInconsistency
    }
}
