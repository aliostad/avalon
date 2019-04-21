using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Avalon.Raft.Core.Rpc;
using Avalon.Raft.Core.Persistence;

namespace Avalon.Raft.Core.Integration
{
    public class Cluster : IDisposable
    {
        private string[] _addresses = Enumerable.Range(1, 5).Select(x => x.ToString()).ToArray(); // 1 to 5
        private readonly ClusterSettings _settings;

        private Dictionary<string, string> _folders = new Dictionary<string, string>();
        private Dictionary<string, DefaultRaftServer> _nodes = new Dictionary<string, DefaultRaftServer>();
        private Dictionary<string, Peer> _peers = new Dictionary<string, Peer>();


        internal Dictionary<string, DefaultRaftServer> Nodes => _nodes;
        internal Dictionary<string, Peer> Peers => _peers;

        public Cluster(ClusterSettings settings)
        {
            _settings = settings;

            SetupFolders();
            SetupPeers();
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
                Directory.Delete(_settings.DataRootFolder, true);
            }

            SafeCreateFolder(_settings.DataRootFolder);
            foreach (var addess in _addresses)
            {
                var path = Path.Combine(_settings.DataRootFolder, addess);
                SafeCreateFolder(path);
                _folders[addess] = path;
            }
        }

        public void Start()
        {
            foreach (var node in _nodes.Values)
                node.Start();
        }

        private void SetupPeers()
        {
            foreach (var address in _addresses)
                _peers.Add(address, new Peer(address, Guid.NewGuid()));

            _peers.Values.ChooseShortNames();
        }
        private void SetupNodes()
        {
            foreach (var address in _addresses)
            {
                var lp = new LmdbPersister(_folders[address], seedId: _peers[address].Id);
                var peers = _peers.Where(x => x.Key != address).Select(x => x.Value).ToArray();
                var peerManager = new PeerManager(peers, _nodes);
                var node = new DefaultRaftServer(lp, lp, lp,
                    new SimpleDictionaryStateMachine(), peerManager, _settings, _peers[address]);
                _nodes.Add(address, node);
            }
        }

        public void Dispose()
        {
            foreach (var node in _nodes.Values)
                node.Dispose();
        }

        class PeerManager : IPeerManager
        {
            private IEnumerable<Peer> _peers;
            private Dictionary<string, DefaultRaftServer> _proxies;

            public IEnumerable<Peer> GetPeers()
            {
                return _peers;
            }

            public IRaftServer GetProxy(string address)
            {
                return _proxies[address];
            }

            public PeerManager(Peer[] peers, Dictionary<string, DefaultRaftServer> proxies)
            {
                _peers = peers;
                _proxies = proxies;
            }
        }
    }
}

