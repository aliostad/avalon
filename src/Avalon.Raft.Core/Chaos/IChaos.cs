using System;
using System.Threading.Tasks;

namespace Avalon.Raft.Core.Chaos
{
    public interface IChaos
    {
        void WreakHavoc();
        Task WreakHavocAsync();
    }

    public class ChaosException : Exception
    {
        public ChaosException() : base("It just happens sometimes...")
        {

        }
    }
}