public class RaftServerSettings
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

}