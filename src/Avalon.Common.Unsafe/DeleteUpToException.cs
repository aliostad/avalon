using System;

namespace Avalon.Raft.Core.Persistence
{
    public class DeleteUpToException : Exception
    {
        public DeleteUpToException(string message, Exception inner) :
            base(message, inner)
        {

        }
    }
}