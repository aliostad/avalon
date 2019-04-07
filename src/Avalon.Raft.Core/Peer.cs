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
        public Peer(string address, Guid id)
        {
            Address = address;
            Id = id;
            ShortName = Id.ToString("N"); // not really short
        }

        /// <summary>
        /// This is either static IP or host name. In current implementation not expected to change should not change.
        /// </summary>
        public string Address { get; private set; }

        /// <summary>
        /// Id of the peer
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// A short name for display purposes
        /// Usually would require to choose a short name by looking at all peers
        /// </summary>
        public string ShortName { get; set; }
    }
}
