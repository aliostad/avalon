using Avalon.Raft.Core.Rpc;
using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// Responsible for providing a list of peers as well as notification when peers change
    /// </summary>
    public interface IPeerManager
    {
        /// <summary>
        /// List peers
        /// </summary>
        /// <returns>List of peers</returns>
        IEnumerable<Peer> GetPeers();

        /// <summary>
        /// Returns an RPC proxy for the peer address
        /// </summary>
        /// <param name="address">Address of the peer (can be IP or hostname, etc; peer manager knows how to handle)</param>
        /// <returns>Proxy to make RPC calls</returns>
        IRaftServer GetProxy(string address);
    }
}
