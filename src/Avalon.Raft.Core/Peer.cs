using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    /// <summary>
    /// A peer Raft server
    /// </summary>
    public class Peer
    {
        /// <summary>
        /// This is either static IP or host name. In current implementation not expected to change should not change.
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Id of the peer
        /// </summary>
        public Guid Id { get; set; }
    }
}
