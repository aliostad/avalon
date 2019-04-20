using Avalon.Raft.Core.Rpc;

namespace Avalon.Raft.Core.Integration
{
    public class ClusterSettings : RaftServerSettings
    {
        public string DataRootFolder { get; set; }

        public bool ClearOldData { get; set; } = true;

    }
}
