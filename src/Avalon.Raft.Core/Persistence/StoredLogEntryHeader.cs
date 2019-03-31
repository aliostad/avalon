using Spreads.Serialization;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Avalon.Raft.Core.Persistence
{
    /// <summary>
    /// The two fields stored before the Raft command buffer in LMDB
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Size = sizeof(long) * 2)]
    [BinarySerialization(sizeof(long) * 2, preferBlittable: false)]
    public struct StoredLogEntryHeader
    {
        /// <summary>
        /// Index of the entry
        /// </summary>
        public long Index;

        /// <summary>
        /// Term of the entry
        /// </summary>
        public long Term;
    }
}
