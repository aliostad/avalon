using System;
using System.Collections.Generic;
using System.Text;

namespace Avalon.Raft.Core
{
    public enum Role
    {
        Follower = 0,
        Candidate,
        Leader
    }
}
