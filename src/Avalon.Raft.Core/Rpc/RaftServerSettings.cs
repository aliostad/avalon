
namespace Avalon.Raft.Core.Rpc
{
    /// <summary>
    /// A superset of RaftSettings adding aspects not explicitly referred to in the Raft paper
    /// </summary>
    public class RaftServerSettings : RaftSettings
    {
        /// <summary>
        /// Whether the State Machine commands should be responded by followers by sending back the address of the leader
        /// In many cases this might not make sense since the address is an internal address unreachable by clients
        /// </summary>
        public bool RedirectStateMachineCommands {get; set;} = false;

        /// <summary>
        /// Whether followers should execute the command by sending to the leader on the client's behalf
        /// </summary>
        public bool ExecuteStateMachineCommandsOnClientBehalf {get; set;} = true;


        /// <summary>
        /// Max size of chunks of snapshot sent in bytes when using InstallSnapshotAsync
        /// </summary>
        public int MaxSnapshotChunkSentInBytes {get; set;} = 256 * 1024; // 256KB
    }
}