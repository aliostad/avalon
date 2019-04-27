using System.Threading.Tasks;

namespace Avalon.Raft.Core.Chaos
{
    public class NoChaos : IChaos
    {
        public void WreakHavoc()
        {
            // none
        }

        public Task WreakHavocAsync()
        {
            return Task.CompletedTask;
        }
    }
}