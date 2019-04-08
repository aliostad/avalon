/// <summary>
/// Request to send a command to change the state machine
/// </summary>
public class StateMachineCommandRequest
{
    /// <summary>
    /// Binary serialised/packed representation of the command 
    /// </summary>
    public byte[] Command {get; set;}
}

public enum CommandOutcome
{
    /// <summary>
    /// Command was successfully accepted although it might not immediately show in the StateMachine
    /// </summary>
    Accepted,

    /// <summary>
    /// Node directs the client to send the request to the address supplied
    /// </summary>
    Redirect,
    
    /// <summary>
    /// Cluster is in disarray or node not aware of the leader and could not redirect.
    /// The client should retry after a period of wait (a few hundred millis).
    /// </summary>
    ServiceUnavailable
}

/// <summary>
/// Response to a command to change the state machine
/// From Raft nodes. Non-leader nodes could simply decide to forward themselves. In this implementation they rather do.
/// </summary>
public class StateMachineCommandResponse
{
    /// <summary>
    /// Outcome of the command
    /// </summary>
    public CommandOutcome Outcome {get; set;}

    /// <summary>
    /// Address of the server the request should be directed to in case outcome is Redirect
    /// </summary>
    public string DirectTo {get; set;}
}