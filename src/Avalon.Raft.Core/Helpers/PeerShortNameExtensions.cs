using System.Collections.Generic;
using System.Linq;

namespace Avalon.Raft.Core
{
    public static class PeerShortNameExtensions
    {
        public static void ChooseShortNames(this IEnumerable<Peer> allPeers)
        {
            var peers = allPeers.ToDictionary(x => x.Id).OrderBy(y => y.Value.Id.ToString());
            var shortNameDics = new Dictionary<string, string>();
            foreach (var peer in peers)
            {
                var i = 0;
                while(true)
                {
                    i++;
                    var shortName = $"{peer.Key.ToString("n").ToUpper().Substring(0,1)}{i}"; // first char plus the number
                    if (!shortNameDics.ContainsKey(shortName))
                    {
                        shortNameDics[shortName] = shortName;
                        peer.Value.ShortName = shortName;
                        break;
                    }
                }
            }
        }

    }
}
