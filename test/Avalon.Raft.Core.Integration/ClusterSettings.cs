namespace Avalon.Raft.Core.Integration
{
    public class ClusterSettings
    {
        public string DataRootFolder { get; set; }

        public bool ClearOldData { get; set; } = true;

    }
}
