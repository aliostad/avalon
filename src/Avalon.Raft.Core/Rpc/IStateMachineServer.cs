using System.Threading.Tasks;

public interface IStateMachineServer
{
    Task<StateMachineCommandResponse> ApplyCommandAsync(StateMachineCommandRequest command);
}