﻿using Paxos.Protocol;
using Paxos.Network;
using Paxos.Persistence;
using Paxos.Notebook;
using Paxos.Message;
using Paxos.MessageDelivery;
using Paxos.Request;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Paxos.Node
{
    public class PaxosNode
    {
        private IMessageTransport _messageTransport;
        private VoterRole _voterRole;
        private ProposerRole _proposerRole;

        private PaxosCluster _cluster;
        private NodeInfo _nodeInfo;

        private PaxosNodeMessageDelivery _messager;

        private Task _messageHandlerTask;


        public PaxosNode(
            IMessageTransport messageTransport,
            PaxosCluster cluster,
            NodeInfo nodeInfo)
        {
            if (messageTransport == null)
            {
                throw new ArgumentNullException("MessageTransport is null");
            }
            if (cluster == null)
            {
                throw new ArgumentNullException("cluster is null");
            }
            if (nodeInfo == null)
            {
                throw new ArgumentNullException("nodeInfo is null");
            }

            _messageTransport = messageTransport;
            _cluster = cluster;
            _nodeInfo = nodeInfo;

            var persistenter = new MemoryPaxosNotePersistent();

            var ledger = new Ledger();
            var voterNote = new VoterNote(persistenter);
            _voterRole = new VoterRole(_nodeInfo, _cluster, _messageTransport, voterNote, ledger);
            var proposerNote = new ProposerNote(ledger);
            _proposerRole = new ProposerRole(_nodeInfo, _cluster, _messageTransport, proposerNote, ledger);

            _messager = new PaxosNodeMessageDelivery(_proposerRole, _voterRole);

            Stop = false;
            _messageHandlerTask = Task.Run(async () =>
            {
                while (!Stop)
                {
                    var message = await messageTransport.ReceiveMessage();
                    await _messager.DeliverMessage(message);
                }
            });

        }

        public Task<ProposeResult> ProposeDecree(PaxosDecree decree, ulong decreeNo)
        {
            //
            // three phase commit
            // 1. collect decree for this instance
            // 2. prepare commit decree
            // 3. commit decree
            //

            return _proposerRole.BeginNewPropose(decree, decreeNo);
        }

        public async Task<DecreeReadResult> ReadDecree(ulong decreeNo)
        {
            var result = await _proposerRole.ReadDecree(decreeNo);
            return result;
        }

        public bool Stop { get; set; }
    }
}
