using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// A Raft snapshot keeping the state of the Raft State Machine as of LastIncludedIndex
    /// </summary>
    public class Snapshot
    {
        /// <summary>
        /// Full name
        /// </summary>
        public string FullName { get; set; }

        /// <summary>
        /// Last index of the log which has been applied to the State Machine
        /// </summary>
        public long LastIncludedIndex { get; set; }

    }
}
