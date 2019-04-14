using System;

namespace Avalon.Raft.Core.Persistence
{
    public class EntriesNotAvailableAnymoreException: Exception
    {
        public EntriesNotAvailableAnymoreException(long firstEntryIndex, long logOffset) :
            base($"Entry no longer available in the log due to snapshotting. firstEntryIndex: {firstEntryIndex} - logOffset: {logOffset}")
        {
            
        }
    }
}