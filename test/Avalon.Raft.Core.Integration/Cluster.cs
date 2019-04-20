using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Avalon.Raft.Core.Rpc;
using Avalon.Raft.Core.Persistence;

namespace Avalon.Raft.Core.Integration
{
    public class Cluster
    {
        private string[] _addresses = Enumerable.Range(1, 5).Select(x => x.ToString()).ToArray(); // 1 to 5

        private Dictionary<string, string> _folders = new Dictionary<string, string>();
        private Dictionary<string, IRaftServer> _nodes = new Dictionary<string, IRaftServer>();
        private Dictionary<string, Peer> _peers = new Dictionary<string, Peer>();

        private readonly ClusterSettings _settings;
        public Cluster(ClusterSettings settings)
        {
            _settings = settings;

            SetupFolders();
            SetupNodes();
        }

        private void SafeCreateFolder(string path)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
        }

        private void SetupFolders()
        {
            if (Directory.Exists(_settings.DataRootFolder) && _settings.ClearOldData)
            {
                Directory.Delete(_settings.DataRootFolder);
            }

            SafeCreateFolder(_settings.DataRootFolder);
            foreach (var addess in _addresses)
            {
                var path = Path.Combine(_settings.DataRootFolder, addess);
                SafeCreateFolder(path);
                _folders[addess] = path;
            }
        }

        private void SetupPeers()
        {
            foreach (var address in _addresses)
                _peers.Add(address, new Peer(address, Guid.NewGuid()));
        }
        private void SetupNodes()
        {
            foreach (var address in _addresses)
            {
                var lp = new LmdbPersister(_folders[address]);
                var peers = _peers.Where(x => x.Key != address).Select(x => x.Value).ToArray();
                var peerManager = new PeerManager(peers, _nodes);
                var node = new DefaultRaftServer(lp, lp, lp,
                    new SimpleDictionaryStateMachine(), peerManager, new RaftServerSettings());
                _nodes.Add(address, node);
            }
        }

        class PeerManager : IPeerManager
        {
            private IEnumerable<Peer> _peers;
            private Dictionary<string, IRaftServer> _proxies;

            public IEnumerable<Peer> GetPeers()
            {
                return _peers;
            }

            public IRaftServer GetProxy(string address)
            {
                return _proxies[address];
            }

            public PeerManager(Peer[] peers, Dictionary<string, IRaftServer> proxies)
            {
                _peers = peers;
                _proxies = proxies;
            }
        }
    }
}

