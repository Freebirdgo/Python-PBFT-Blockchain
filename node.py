#! /usr/bin/env python3
import logging, traceback
import argparse
import yaml
import time
from random import random, randint
from collections import Counter
import json
import sys

import asyncio
import aiohttp
from aiohttp import web

import hashlib

VIEW_SET_INTERVAL = 10

class View:
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
        # Minimum interval to set the view number
        self._min_set_interval = VIEW_SET_INTERVAL
        self._last_set_time = time.time()

    # To encode to json
    def get_view(self):
        return self._view_number

    # Recover from json data.
    def set_view(self, view):
        '''
        Retrun True if successfully update view number
        return False otherwise.
        '''
        if time.time() - self._last_set_time < self._min_set_interval:
            return False
        self._last_set_time = time.time()
        self._view_number = view
        self._leader = view % self._num_nodes
        return True

    def get_leader(self):
        return self._leader

class Status:
    '''
    Record the state for every slot.
    '''
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = "reply"

    def __init__(self, f):
        self.f = f
        self.request = 0
        self.prepare_msgs = {}
        self.prepare_certificate = None # proposal
        self.commit_msgs = {}
        # Only means receive more than 2f + 1 commit message,
        # but can not commit if there are any bubbles previously.
        self.commit_certificate = None # proposal

        # Set it to True only after commit
        self.is_committed = False

    class Certificate:
        def __init__(self, view, proposal = 0):
            '''
            input:
                view: object of class View
                proposal: proposal in json_data(dict)
            '''
            self._view = view
            self._proposal = proposal

        def to_dict(self):
            '''
            Convert the Certificate to dictionary
            '''
            return {
                'view': self._view.get_view(),
                'proposal': self._proposal
            }

        def dumps_from_dict(self, dictionary):
            '''
            Update the view from the form after self.to_dict
            input:
                dictionay = {
                    'view': self._view.get_view(),
                    'proposal': self._proposal
                }
            '''
            self._view.set_view(dictionary['view'])
            self._proposal = dictionary['proposal']
        def get_proposal(self):
            return self._proposal


    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):
        '''
        Update the record in the status by message type
        input:
            msg_type: Status.PREPARE or Status.COMMIT
            view: View object of self._follow_view
            proposal: proposal in json_data
            from_node: The node send given the message.
        '''

        # The key need to include hash(proposal) in case get different
        # preposals from BFT nodes. Need sort key in json.dumps to make
        # sure getting the same string. Use hashlib so that we got same
        # hash everytime.
        hash_object = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get_view(), hash_object.digest())
        if msg_type == Status.PREPARE:
            if key not in self.prepare_msgs:
                self.prepare_msgs[key] = self.SequenceElement(proposal)
            self.prepare_msgs[key].from_nodes.add(from_node)
        elif msg_type == Status.COMMIT:
            if key not in self.commit_msgs:
                self.commit_msgs[key] = self.SequenceElement(proposal)
            self.commit_msgs[key].from_nodes.add(from_node)

    def _check_majority(self, msg_type):
        '''
        Check if receive more than 2f + 1 given type message in the same view.
        input:
            msg_type: self.PREPARE or self.COMMIT
        '''
        if msg_type == Status.PREPARE:
            if self.prepare_certificate:
                return True
            for key in self.prepare_msgs:
                if len(self.prepare_msgs[key].from_nodes)>= 2 * self.f + 1:
                    return True
            return False

        if msg_type == Status.COMMIT:
            if self.commit_certificate:
                return True
            for key in self.commit_msgs:
                if len(self.commit_msgs[key].from_nodes) >= 2 * self.f + 1:
                    return True
            return False

class CheckPoint:
    '''
    Record all the status of the checkpoint for given PBFTHandler.
    '''
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    def __init__(self, checkpoint_interval, nodes, f, node_index,
            lose_rate = 0, network_timeout = 10):
        self._checkpoint_interval = checkpoint_interval
        self._nodes = nodes
        self._f = f
        self._node_index = node_index
        self._loss_rate = lose_rate
        self._log = logging.getLogger(f"Node[{self._node_index}].CheckPoint") # More specific logger
        # Next slot of the given globally accepted checkpoint.
        # For example, the current checkpoint record until slot 99
        # next_slot = 100
        self.next_slot = 0
        # Globally accepted checkpoint
        self.checkpoint = []
        # Use the hash of the checkpoint to record receive votes for given ckpt.
        self._received_votes_by_ckpt = {}
        self._session = None
        self._network_timeout = network_timeout

        self._log.info("---> Create checkpoint.")

    # Class to record the status of received checkpoints
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot):
            self.from_nodes = set([])
            self.checkpoint = ckpt
            self.next_slot = next_slot

    def get_commit_upperbound(self):
        '''
        Return the upperbound that could commit
        (return upperbound = true upperbound + 1)
        '''
        return self.next_slot + 2 * self._checkpoint_interval

    def _hash_ckpt(self, ckpt):
        '''
        input:
            ckpt: the checkpoint
        output:
            The hash of the input checkpoint in the format of
            binary string.
        '''
        hash_object = hashlib.sha256(json.dumps(ckpt, sort_keys=True).encode())
        return hash_object.digest()


    async def receive_vote(self, ckpt_vote):
        '''
        Trigger when PBFTHandler receive checkpoint votes.
        First, we update the checkpoint status. Second,
        update the checkpoint if more than 2f + 1 node
        agree with the given checkpoint.
        input:
            ckpt_vote = {
                'node_index': self._node_index
                'next_slot': self._next_slot + self._checkpoint_interval
                'ckpt': json.dumps(ckpt)
                'type': 'vote'
            }
        '''
        self._log.debug("Receive checkpoint votes")
        ckpt = json.loads(ckpt_vote['ckpt'])
        next_slot_val = ckpt_vote['next_slot'] # Renamed to avoid conflict
        from_node = ckpt_vote['node_index']

        hash_ckpt = self._hash_ckpt(ckpt)
        if hash_ckpt not in self._received_votes_by_ckpt:
            self._received_votes_by_ckpt[hash_ckpt] = (
                CheckPoint.ReceiveVotes(ckpt, next_slot_val))
        status = self._received_votes_by_ckpt[hash_ckpt]
        status.from_nodes.add(from_node)
        
        updated_ckpt = False
        for current_hash_ckpt in list(self._received_votes_by_ckpt.keys()): # Iterate over a copy of keys
            if current_hash_ckpt in self._received_votes_by_ckpt: # Check if key still exists
                vote_status = self._received_votes_by_ckpt[current_hash_ckpt]
                if (vote_status.next_slot > self.next_slot and
                        len(vote_status.from_nodes) >= 2 * self._f + 1):
                    self._log.info("Update checkpoint by receiving votes. New next_slot: %d", vote_status.next_slot)
                    self.next_slot = vote_status.next_slot
                    self.checkpoint = vote_status.checkpoint
                    updated_ckpt = True
        
        if updated_ckpt: # Perform garbage collection for votes if checkpoint updated
            await self.garbage_collection_votes()


    async def propose_vote(self, commit_decisions):
        '''
        When node the slots of committed message exceed self._next_slot
        plus self._checkpoint_interval, propose new checkpoint and
        broadcast to every node

        input:
            commit_decisions: list of tuple: [((client_index, client_seq), data), ... ]

        output:
            next_slot for the new update and garbage collection of the Status object.
        '''
        proposed_checkpoint = self.checkpoint + commit_decisions
        await self._broadcast_checkpoint(proposed_checkpoint,
            'vote', CheckPoint.RECEIVE_CKPT_VOTE)


    async def _post(self, nodes, command, json_data):
        '''
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: Data in json format.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=self._network_timeout) # Use total for overall timeout
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to node %d (%s:%s), command: %s", i, node['host'], node['port'], command)
                try:
                    _ = await self._session.post(
                        self.make_url(node, command), json=json_data)
                except Exception as e:
                    self._log.error(f"Failed to post to node {i} ({node['host']}:{node['port']}) command {command}: {e}")
                    pass

    @staticmethod
    def make_url(node, command):
        '''
        input:
            node: dictionary with key of host(url) and port
            command: action
        output:
            The url to send with given node and action.
        '''
        return "http://{}:{}/{}".format(node['host'], node['port'], command)


    async def _broadcast_checkpoint(self, ckpt, msg_type, command):
        json_data = {
            'node_index': self._node_index,
            'next_slot': self.next_slot + self._checkpoint_interval,
            'ckpt': json.dumps(ckpt),
            'type': msg_type
        }
        await self._post(self._nodes, command, json_data)

    def get_ckpt_info(self):
        '''
        Get the checkpoint serialized information.Called
        by synchronize function to get the checkpoint
        information.
        '''
        json_data = {
            'next_slot': self.next_slot,
            'ckpt': json.dumps(self.checkpoint)
        }
        return json_data

    def update_checkpoint(self, json_data):
        '''
        Update the checkpoint when input checkpoint cover
        more slots than current.
        input:
            json_data = {
                'next_slot': self._next_slot
                'ckpt': json.dumps(ckpt)
            }
        '''
        self._log.debug("update_checkpoint: current next_slot: %d; received next_slot: %d"
            , self.next_slot, json_data['next_slot'])
        if json_data['next_slot'] > self.next_slot:
            self._log.info("Update checkpoint by synchronization. New next_slot: %d", json_data['next_slot'])
            self.next_slot = json_data['next_slot']
            self.checkpoint = json.loads(json_data['ckpt'])


    async def receive_sync(self, sync_ckpt_data): # Renamed parameter
        '''
        Trigger when receive checkpoint synchronization messages.
        input:
            sync_ckpt_data = { # Changed from json_data to avoid confusion
                'node_index': ...,
                'next_slot': ...,
                'ckpt': json.dumps(ckpt),
                'type': 'sync'
            }
        '''
        # This method seems to be part of PBFTHandler, not CheckPoint.
        # Assuming this method is intended to be in PBFTHandler and called
        # from self._ckpt.update_checkpoint or similar.
        # For now, this method in CheckPoint class is not directly called by PBFTHandler's receive_sync
        # If it's meant to be called directly on CheckPoint, it needs self as first param.
        # The parameter `json_data` was undefined in the original, changed to `sync_ckpt_data`
        self._log.debug("receive_sync in checkpoint: current next_slot:"
            " %d; update to: %d" , self.next_slot, sync_ckpt_data['next_slot']) # used sync_ckpt_data

        if sync_ckpt_data['next_slot'] > self.next_slot: # Changed from self._next_slot to self.next_slot
            self.next_slot = sync_ckpt_data['next_slot']
            self.checkpoint = json.loads(sync_ckpt_data['ckpt'])

    async def garbage_collection_votes(self): # Renamed from garbage_collection to be specific
        '''
        Clean those ReceiveCKPT objects whose next_slot smaller
        than or equal to the current self.next_slot.
        '''
        deletes = []
        for hash_ckpt_key in self._received_votes_by_ckpt: # Renamed hash_ckpt to hash_ckpt_key
            if self._received_votes_by_ckpt[hash_ckpt_key].next_slot <= self.next_slot: # Used self.next_slot
                deletes.append(hash_ckpt_key)
        for hash_ckpt_key_to_delete in deletes: # Renamed hash_ckpt to hash_ckpt_key_to_delete
            if hash_ckpt_key_to_delete in self._received_votes_by_ckpt: # Check before deleting
                del self._received_votes_by_ckpt[hash_ckpt_key_to_delete]
        if deletes:
            self._log.debug("Garbage collected %d checkpoint vote entries.", len(deletes))


class ViewChangeVotes:
    """
    Record which nodes vote for the proposed view change.
    In addition, store all the information including:
    (1)checkpoints who has the largest information(largest
    next_slot) (2) prepare certificate with largest for each
    slot sent from voted nodes.
    """
    def __init__(self, node_index, num_total_nodes):
        # Current node index.
        self._node_index = node_index
        # Total number of node in the system.
        self._num_total_nodes = num_total_nodes
        # Number of faults tolerand
        self._f = (self._num_total_nodes - 1) // 3
        # Record the which nodes vote for current view.
        self.from_nodes = set([])
        # The prepare_certificate with highest view for each slot
        self.prepare_certificate_by_slot = {}
        self.lastest_checkpoint = None # This should store checkpoint data, not just be None
        self._log = logging.getLogger(f"Node[{self._node_index}].ViewChangeVotes")

    def receive_vote(self, json_data):
        '''
        Receive the vote message and make the update:
        (1) Update the inforamtion in given vote storage -
        prepare certificate.(2) update the node in from_nodes.
        input:
            json_data: the json_data received by view change vote broadcast:
                {
                    "node_index": self._index,
                    "view_number": self._follow_view.get_view(),
                    "checkpoint":self._ckpt.get_ckpt_info(),
                    "prepared_certificates":self.get_prepare_certificates(),
                }
        '''
        # update_view = None # This variable was unused

        prepare_certificates = json_data["prepare_certificates"]

        self._log.debug("Update prepare_certificate for view %d from node %d",
            json_data['view_number'], json_data['node_index'])

        for slot_str, cert_dict in prepare_certificates.items(): # Iterate over items
            # slot is already a string from json key
            prepare_certificate = Status.Certificate(View(0, self._num_total_nodes)) # Temp view
            prepare_certificate.dumps_from_dict(cert_dict)
            # Keep the prepare certificate who has the largest view number
            if slot_str not in self.prepare_certificate_by_slot or (
                    self.prepare_certificate_by_slot[slot_str]._view.get_view() < (
                    prepare_certificate._view.get_view())):
                self.prepare_certificate_by_slot[slot_str] = prepare_certificate

        # Update latest_checkpoint
        voter_checkpoint_info = json_data["checkpoint"]
        if self.lastest_checkpoint is None or voter_checkpoint_info['next_slot'] > self.lastest_checkpoint['next_slot']:
            self.lastest_checkpoint = voter_checkpoint_info


        self.from_nodes.add(json_data['node_index'])


class Block:
    def __init__(self, index, transactions, timestamp, previous_hash):
        self.index          = index
        self.transactions   = transactions
        self.timestamp      = timestamp
        self.hash           = ''
        self.previous_hash  = previous_hash

    def compute_hash(self):
        """
        A function that return the hash of the block contents.
        """
        block_string = json.dumps(self.__dict__, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()
    def get_json(self):
        return json.dumps(self.__dict__ , indent=4, sort_keys=True)


class Blockchain:
    def __init__(self):
        self.commit_counter = 0 # Tracks blocks written to file vs in-memory chain
        self.length = 0
        self.chain = []
        self.create_genesis_block()

    def create_genesis_block(self):
        """
        A function to generate genesis block and appends it to
        the chain. The block has index 0, previous_hash as 0, and
        a valid hash.
        """
        genesis_block = Block(0, ["Genenesis Block"], 0, "0") # Added time.time() for timestamp
        genesis_block.hash = genesis_block.compute_hash()
        self.length += 1
        self.chain.append(genesis_block)

    @property # Made it a property for easier access
    def last_block(self):
        return self.chain[-1]

    def last_block_hash(self):
        # tail = self.chain[-1] # Redundant if using self.last_block
        return self.last_block.hash

    def update_commit_counter(self): # To be called after writing to file
        self.commit_counter = self.length


    def add_block(self, block):
        """
        A function that adds the block to the chain after verification.
        Verification includes:
        * The previous_hash referred in the block and the hash of latest block
          in the chain match.
        """
        previous_hash = self.last_block_hash()

        if previous_hash != block.previous_hash:
            logging.error(f"Block prev_hash mismatch: expected {previous_hash}, got {block.previous_hash}") # Added logging
            raise Exception('block.previous_hash not equal to last_block_hash')
            return

        block.hash = block.compute_hash()
        self.length += 1
        self.chain.append(block)



class PBFTHandler:
    REQUEST = 'request'
    PREPREPARE = 'preprepare'
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = 'reply'

    NO_OP = 'NOP'

    RECEIVE_SYNC = 'receive_sync'
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'

    VIEW_CHANGE_REQUEST = 'view_change_request'
    VIEW_CHANGE_VOTE = "view_change_vote"

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._index = index
        self._log = logging.getLogger(f"Node[{self._index}]") # Specific logger for this node
        # Number of faults tolerant.
        self._f = (self._node_cnt - 1) // 3

        # leader
        self._view = View(0, self._node_cnt)
        self._next_propose_slot = 0 # For primary, next slot to assign

        self._blockchain =  Blockchain()

        # tracks if commit_decisions had been commited to blockchain for the current interval
        self.committed_this_interval_to_blockchain = False # Renamed for clarity

        self._is_leader = (self._index == self._view.get_leader()) # Dynamic leader check

        # Network simulation
        self._loss_rate = conf['loss%'] / 100

        # Time configuration
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpoint
        self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes,
            self._f, self._index, self._loss_rate, self._network_timeout)
        # Commit
        self._last_commit_slot = -1 # Highest slot number that this node has committed locally

        # Follow view: Tracks the view this node believes is current, especially during view changes
        self._follow_view = View(0, self._node_cnt)
        # Store ViewChangeVotes objects keyed by the view number they are for
        self._view_change_votes_by_view_number = {}

        # Record all the status of the given slot
        # To adjust json key, slot is string integer.
        self._status_by_slot = {} # slot_str -> Status object

        self._sync_interval = conf['sync_interval']

        self._session = None # aiohttp.ClientSession, initialized on first use


    @staticmethod
    def make_url(node, command):
        return "http://{}:{}/{}".format(node['host'], node['port'], command)

    async def _ensure_session(self):
        if not self._session or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)


    async def _post(self, nodes, command, json_data):
        await self._ensure_session()
        tasks = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                url = self.make_url(node, command)
                self._log.debug("POST to node %d (%s), command: %s, data: %s", i, url, command, str(json_data)[:100])
                # Create a task for each post, but don't await here, gather results later
                tasks.append(asyncio.create_task(self._session.post(url, json=json_data)))
            else:
                self._log.debug("Simulating message loss to node %d for command %s", i, command)

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for i, resp_or_exc in enumerate(responses):
            if isinstance(resp_or_exc, Exception):
                # Find the original node index this task corresponded to,
                # this part is tricky if tasks list doesn't map 1:1 to nodes due to loss_rate
                # For simplicity, just log the error. A more robust mapping would be needed if per-node failure is critical here.
                self._log.error(f"Error posting command {command} to a node: {resp_or_exc}")
            # else:
                # self._log.debug(f"Successfully posted command {command} to a node, status: {resp_or_exc.status}")


    def _legal_slot(self, slot_str): # slot is expected to be string
        slot = int(slot_str)
        is_legal = self._ckpt.next_slot <= slot < self._ckpt.get_commit_upperbound()
        if not is_legal:
            self._log.warning(f"Slot {slot_str} is illegal. Checkpoint next_slot: {self._ckpt.next_slot}, commit_upperbound: {self._ckpt.get_commit_upperbound()}")
        return is_legal

    async def preprepare(self, client_request_data):
        # This is called by the primary when it receives a client request.
        this_slot_int = self._next_propose_slot
        this_slot_str = str(this_slot_int)
        self._next_propose_slot = this_slot_int + 1

        self._log.info("On preprepare, proposing for slot: %s", this_slot_str)

        if this_slot_str not in self._status_by_slot:
            self._status_by_slot[this_slot_str] = Status(self._f)
        self._status_by_slot[this_slot_str].request = client_request_data # Store the original client request

        preprepare_msg = {
            'leader': self._index, # This node is the leader proposing
            'view': self._view.get_view(),
            'proposal': {
                this_slot_str: client_request_data # The actual client request data becomes the proposal for this slot
            },
            'type': 'preprepare' # Message type for routing/handling
        }

        await self._post(self._nodes, PBFTHandler.PREPREPARE, preprepare_msg) # Send to /prepare endpoint of other nodes

    async def get_request(self, request: web.Request): # Type hint for clarity
        # Handles initial client request
        self._is_leader = (self._index == self._view.get_leader()) # Re-check if leader, view might have changed
        self._log.info("Received client request. Am I leader? %s (View: %d, Leader for view: %d)",
                       self._is_leader, self._view.get_view(), self._view.get_leader())


        if not self._is_leader:
            current_leader_id = self._view.get_leader()
            if current_leader_id is not None and 0 <= current_leader_id < self._node_cnt:
                leader_node_info = self._nodes[current_leader_id]
                leader_url = self.make_url(leader_node_info, PBFTHandler.REQUEST)
                self._log.info(f"Not leader, redirecting to leader {current_leader_id} at {leader_url}")
                raise web.HTTPTemporaryRedirect(location=leader_url)
            else:
                self._log.error("Cannot determine current leader or leader ID is invalid.")
                raise web.HTTPServiceUnavailable(reason="Current leader unknown or invalid.")
        else:
            json_data = await request.json()
            self._log.debug("Leader received request data: %s", json_data)
            await self.preprepare(json_data) # Pass the client's JSON data
            return web.Response(text="Request received by leader and pre-prepare initiated.")


    async def prepare(self, request: web.Request): # Handles PRE-PREPARE messages
        json_data = await request.json()
        sender_view = json_data['view']
        leader_id = json_data['leader']
        proposal_dict = json_data['proposal'] # This is {slot_str: client_request_data}

        self._log.info("Received preprepare message from leader %d for view %d", leader_id, sender_view)

        if sender_view < self._follow_view.get_view():
            self._log.warning("Ignoring PRE-PREPARE from older view %d (current follow_view: %d)",
                              sender_view, self._follow_view.get_view())
            return web.Response()

        # Potentially update follow_view if a message from a future valid view arrives from the correct leader
        # This logic might need refinement for robust view synchronization
        if sender_view > self._follow_view.get_view() and leader_id == self._follow_view.get_leader(sender_view) : #TODO: get_leader with param
             self._follow_view.set_view(sender_view)
             self._is_leader = (self._index == self._follow_view.get_leader())
             self._log.info(f"Updated follow_view to {sender_view} based on PRE-PREPARE.")


        for slot_str, proposal_data in proposal_dict.items():
            if not self._legal_slot(slot_str):
                self._log.warning("PRE-PREPARE for illegal slot %s, skipping.", slot_str)
                continue

            if slot_str not in self._status_by_slot:
                self._status_by_slot[slot_str] = Status(self._f)
            # Store the original client request if this is the first time we see it for this slot
            # (e.g. if we are a backup and received this PRE-PREPARE)
            if not self._status_by_slot[slot_str].request:
                 self._status_by_slot[slot_str].request = proposal_data


            # Now broadcast PREPARE message
            prepare_msg = {
                'index': self._index,
                'view': sender_view, # Use the view from the PRE-PREPARE
                'proposal': { # The proposal is the client_request_data for that slot
                    slot_str: proposal_data
                },
                'type': Status.PREPARE # This is a PREPARE type message
            }
            self._log.info("Sending PREPARE for slot %s, view %d", slot_str, sender_view)
            await self._post(self._nodes, PBFTHandler.PREPARE, prepare_msg) # Send to /commit endpoint
        return web.Response()

    async def commit(self, request: web.Request): # Handles PREPARE messages
        json_data = await request.json()
        sender_index = json_data['index']
        sender_view = json_data['view']
        proposal_dict = json_data['proposal']

        self._log.info("Received PREPARE message from node %d for view %d", sender_index, sender_view)

        if sender_view < self._follow_view.get_view():
            self._log.warning("Ignoring PREPARE from older view %d", sender_view)
            return web.Response()

        for slot_str, proposal_data in proposal_dict.items():
            if not self._legal_slot(slot_str):
                self._log.warning("PREPARE for illegal slot %s, skipping.", slot_str)
                continue

            if slot_str not in self._status_by_slot:
                self._status_by_slot[slot_str] = Status(self._f)
            status = self._status_by_slot[slot_str]

            # Ensure we have the original request if we are processing prepares for it
            if not status.request:
                status.request = proposal_data # Store it if missing

            current_view_obj = View(sender_view, self._node_cnt) # Create a View object for this message
            status._update_sequence(Status.PREPARE, # Message type is PREPARE
                current_view_obj, proposal_data, sender_index)

            if not status.prepare_certificate and status._check_majority(Status.PREPARE): # Check if we have 2f+1 PREPAREs
                self._log.info("Prepare certificate formed for slot %s, view %d", slot_str, sender_view)
                status.prepare_certificate = Status.Certificate(current_view_obj, proposal_data)

                # If prepared, send COMMIT message
                commit_msg = {
                    'index': self._index,
                    'view': sender_view,
                    'proposal': { # Proposal is client_request_data for that slot
                        slot_str: proposal_data
                    },
                    'type': Status.COMMIT # This is a COMMIT type message
                }
                self._log.info("Sending COMMIT for slot %s, view %d", slot_str, sender_view)
                await self._post(self._nodes, PBFTHandler.COMMIT, commit_msg) # Send to /reply endpoint
        return web.Response()

    async def reply(self, request: web.Request): # Handles COMMIT messages
        json_data = await request.json()
        sender_index = json_data['index']
        sender_view = json_data['view']
        proposal_dict = json_data['proposal']

        self._log.info("Received COMMIT message from node %d for view %d", sender_index, sender_view)

        if sender_view < self._follow_view.get_view():
            self._log.warning("Ignoring COMMIT from older view %d", sender_view)
            return web.Response()

        for slot_str, proposal_data in proposal_dict.items():
            if not self._legal_slot(slot_str):
                self._log.warning("COMMIT for illegal slot %s, skipping.", slot_str)
                continue

            if slot_str not in self._status_by_slot:
                self._status_by_slot[slot_str] = Status(self._f)
            status = self._status_by_slot[slot_str]

            if not status.request: # Should ideally have the request from pre-prepare/prepare stage
                status.request = proposal_data

            current_view_obj = View(sender_view, self._node_cnt)
            status._update_sequence(Status.COMMIT, # Message type is COMMIT
                current_view_obj, proposal_data, sender_index)

            if not status.commit_certificate and status._check_majority(Status.COMMIT): # Check if we have 2f+1 COMMITs
                self._log.info("Commit certificate formed for slot %s, view %d", slot_str, sender_view)
                status.commit_certificate = Status.Certificate(current_view_obj, proposal_data)

                # Attempt to commit this slot if it's the next expected one
                await self._try_local_commit_and_reply()
        return web.Response()

    async def _try_local_commit_and_reply(self):
        # This function tries to commit slots in sequence
        while True:
            next_slot_to_commit_int = self._last_commit_slot + 1
            next_slot_to_commit_str = str(next_slot_to_commit_int)

            if next_slot_to_commit_str in self._status_by_slot:
                status = self._status_by_slot[next_slot_to_commit_str]
                if status.commit_certificate and not status.is_committed:
                    self._log.info("Locally committing slot %s", next_slot_to_commit_str)
                    status.is_committed = True
                    self._last_commit_slot = next_slot_to_commit_int # Update last committed slot

                    # Send REPLY to client
                    client_request_data = status.commit_certificate.get_proposal()
                    reply_msg = {
                        'index': self._index,
                        'view': status.commit_certificate._view.get_view(), # Use view from cert
                        'proposal': client_request_data, # This is the original client request
                        'type': Status.REPLY
                    }
                    client_url = client_request_data.get('client_url')
                    if client_url:
                        try:
                            await self._ensure_session()
                            await self._session.post(client_url, json=reply_msg)
                            self._log.info("Replied to client for slot %s successfully!", next_slot_to_commit_str)
                        except Exception as e:
                            self._log.error(f"Send REPLY failed to {client_url} for slot {next_slot_to_commit_str}: {e}")
                    else:
                        self._log.warning(f"No client_url in proposal for slot {next_slot_to_commit_str}, cannot send reply.")


                    # Check if checkpoint interval is reached
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        self._log.info("Checkpoint interval reached at slot %d.", self._last_commit_slot)
                        commit_decisions_for_ckpt = self.get_commit_decisions_for_checkpoint()
                        await self._ckpt.propose_vote(commit_decisions_for_ckpt)
                        self.committed_this_interval_to_blockchain = False # Reset for next interval
                        await self._commit_action_to_blockchain() # Add block after proposing vote
                else:
                    break # Next slot not ready or already committed
            else:
                break # Next slot status not found

    def get_commit_decisions_for_checkpoint(self):
        '''
        Get the committed proposals for the just-completed checkpoint interval.
        '''
        commit_decisions = []
        # Interval ends at _last_commit_slot, starts at _last_commit_slot - _checkpoint_interval + 1
        # But Checkpoint object stores from its self.next_slot.
        # So we need decisions from self._ckpt.next_slot up to self._last_commit_slot
        start_slot_for_ckpt = self._ckpt.next_slot
        end_slot_for_ckpt = self._last_commit_slot

        self._log.debug("Getting commit decisions for checkpoint: from slot %d to %d", start_slot_for_ckpt, end_slot_for_ckpt)

        for i in range(start_slot_for_ckpt, end_slot_for_ckpt + 1):
            slot_str = str(i)
            if slot_str in self._status_by_slot:
                status = self._status_by_slot[slot_str]
                if status.commit_certificate and status.is_committed: # Ensure it was locally committed
                    proposal = status.commit_certificate.get_proposal()
                    commit_decisions.append((str(proposal['id']), proposal['data']))
                else:
                    self._log.warning(f"Slot {slot_str} expected in checkpoint interval but not committed or no certificate.")
            else:
                 self._log.warning(f"Slot {slot_str} expected in checkpoint interval but no status found.")
        return commit_decisions

    async def _commit_action_to_blockchain(self):
        '''
        Gathers committed transactions for the completed interval and adds a block.
        Writes new blocks to file.
        '''
        if self.committed_this_interval_to_blockchain:
            self._log.debug("Block for this interval already processed.")
            return

        # These are the transactions for the block being formed NOW.
        # It should be the set of transactions from the just completed checkpoint interval.
        # Checkpoint's self.checkpoint is the *previous* stable state.
        # We need decisions from self._ckpt.next_slot (start of new interval) to current self._last_commit_slot
        current_interval_decisions = []
        start_slot_of_new_block = self._last_commit_slot - self._checkpoint_interval + 1
        if self._last_commit_slot +1 < self._checkpoint_interval : # handles the first block if interval not fully met
             start_slot_of_new_block = 0


        for i in range(start_slot_of_new_block, self._last_commit_slot + 1):
            slot_str = str(i)
            if slot_str in self._status_by_slot and \
               self._status_by_slot[slot_str].commit_certificate and \
               self._status_by_slot[slot_str].is_committed:
                proposal = self._status_by_slot[slot_str].commit_certificate.get_proposal()
                current_interval_decisions.append((str(proposal['id']), proposal['data']))
            else: #This should not happen if _try_local_commit_and_reply works correctly for sequence
                self._log.warning(f"During block formation, slot {slot_str} was expected but not fully committed.")


        if not current_interval_decisions and self._blockchain.length > 1 : # Don't add empty blocks unless it's not genesis
            self._log.info("No committed decisions for the current interval to add to blockchain.")
            # We still need to mark that we 'processed' this interval to allow next one.
            # Or, only set this true if a block *is* added.
            # self.committed_this_interval_to_blockchain = True # Let's set it true only if block added
            return


        self._log.info(f"Adding new block to blockchain with {len(current_interval_decisions)} transactions.")
        try:
            # Get timestamp from the last proposal in the interval, or current time
            last_proposal_in_interval = None
            if self._status_by_slot.get(str(self._last_commit_slot)):
                 if self._status_by_slot[str(self._last_commit_slot)].commit_certificate:
                      last_proposal_in_interval = self._status_by_slot[str(self._last_commit_slot)].commit_certificate.get_proposal()

            timestamp_val = time.time() # Default to current time
            if last_proposal_in_interval and 'timestamp' in last_proposal_in_interval:
                try:
                    # The original code uses time.asctime(time.localtime(proposal['timestamp']))
                    # For consistency, ensure proposal['timestamp'] is a float (Unix timestamp)
                    ts_from_proposal = last_proposal_in_interval['timestamp']
                    if isinstance(ts_from_proposal, (int,float)):
                         timestamp_val = ts_from_proposal
                    else: #If it's a string like "no_op" or malformed
                         self._log.warning(f"Invalid timestamp in proposal for block: {ts_from_proposal}. Using current time.")
                except Exception as e:
                    self._log.error(f"Error processing timestamp from proposal: {e}. Using current time.")

            new_block = Block(self._blockchain.length,
                              current_interval_decisions,
                              timestamp_val, # Use the determined timestamp
                              self._blockchain.last_block_hash())
            self._blockchain.add_block(new_block)
            self.committed_this_interval_to_blockchain = True # Mark interval as processed for blockchain
            self._log.info(f"Added Block {new_block.index} with hash {new_block.hash[:8]}...")


            # Write new blocks to file
            # self._blockchain.commit_counter was how many blocks were previously written.
            # self._blockchain.length is total blocks in memory.
            # We want to write blocks from self._blockchain.commit_counter up to self._blockchain.length -1
            with open(f"~$node_{self._index}.blockchain", 'a') as f:
                for i in range(self._blockchain.commit_counter, self._blockchain.length):
                    f.write(str(self._blockchain.chain[i].get_json()) + '\n------------\n')
                self._blockchain.update_commit_counter() # This sets commit_counter = length
            self._log.info(f"Written blocks up to index {self._blockchain.length -1} to file.")

        except Exception as e:
            self._log.error("Error during _commit_action_to_blockchain: %s", e, exc_info=True)


    async def receive_ckpt_vote(self, request: web.Request):
        self._log.info("Received checkpoint vote.")
        json_data = await request.json()
        await self._ckpt.receive_vote(json_data)
        return web.Response()

    async def receive_sync(self, request: web.Request):
        self._log.debug("On receive_sync.")
        json_data = await request.json()

        # Update checkpoint first
        self._ckpt.update_checkpoint(json_data['checkpoint'])
        # After updating checkpoint, our _last_commit_slot might be lagging.
        # We should ensure it's at least up to the new checkpoint's start.
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        self.committed_this_interval_to_blockchain = False # Reset as state might change significantly

        # Update commit certificates for slots based on sync message
        commit_certificates_received = json_data['commit_certificates']
        for slot_str, cert_dict in commit_certificates_received.items():
            slot_int = int(slot_str)
            # Only process slots that are now legal and potentially missing a certificate
            if slot_int >= self._ckpt.get_commit_upperbound() or slot_int < self._ckpt.next_slot:
                continue

            if slot_str not in self._status_by_slot:
                self._status_by_slot[slot_str] = Status(self._f)

            status = self._status_by_slot[slot_str]
            if not status.commit_certificate: # Only update if we don't have one
                commit_certificate = Status.Certificate(View(0, self._node_cnt)) # Temp view
                commit_certificate.dumps_from_dict(cert_dict)
                status.commit_certificate = commit_certificate
                status.request = commit_certificate.get_proposal() # Ensure request is populated
                self._log.debug(f"Updated commit certificate for slot {slot_str} from sync.")

        # After processing all received certificates, try to commit locally
        await self._try_local_commit_and_reply()
        # After local commits, also try to form a block if an interval was completed
        if (self._last_commit_slot + 1) % self._checkpoint_interval == 0 and self._last_commit_slot >= 0 :
             if not self.committed_this_interval_to_blockchain :
                  await self._commit_action_to_blockchain()


        return web.Response()


    async def synchronize(self):
        while True:
            await asyncio.sleep(self._sync_interval)
            self._log.debug("Initiating periodic synchronize.")
            commit_certificates_to_send = {}
            # Send commit certificates for slots within our current "working window"
            # This window is from our stable checkpoint up to where we might have proposals
            for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
                slot_str = str(i)
                if (slot_str in self._status_by_slot and
                        self._status_by_slot[slot_str].commit_certificate):
                    status = self._status_by_slot[slot_str]
                    commit_certificates_to_send[slot_str] = status.commit_certificate.to_dict()
            sync_data = {
                'node_index': self._index, # Added sender index
                'checkpoint': self._ckpt.get_ckpt_info(),
                'commit_certificates': commit_certificates_to_send
            }
            await self._post(self._nodes, PBFTHandler.RECEIVE_SYNC, sync_data)

    async def get_prepare_certificates_for_view_change(self): # Renamed for clarity
        prepare_certificate_by_slot = {}
        # Iterate over a relevant range of slots, e.g., current checkpoint window
        for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
            slot_str = str(i)
            if slot_str in self._status_by_slot:
                status = self._status_by_slot[slot_str]
                if status.prepare_certificate:
                    prepare_certificate_by_slot[slot_str] = (
                        status.prepare_certificate.to_dict())
        return prepare_certificate_by_slot

    async def _post_view_change_vote(self):
        # This node is voting for self._follow_view
        prepare_certs = await self.get_prepare_certificates_for_view_change()
        view_change_vote_msg = {
            "node_index": self._index,
            "view_number": self._follow_view.get_view(), # The view being voted for
            "checkpoint":self._ckpt.get_ckpt_info(),
            "prepare_certificates": prepare_certs,
        }
        self._log.info(f"Broadcasting VIEW_CHANGE_VOTE for view {self._follow_view.get_view()}")
        await self._post(self._nodes, PBFTHandler.VIEW_CHANGE_VOTE, view_change_vote_msg)

    async def get_view_change_request(self, request: web.Request):
        self._log.info("Received view change request from client.")
        json_data = await request.json()
        if json_data.get('action') != "view change":
            self._log.warning("Invalid view change request action.")
            return web.Response(status=400, text="Invalid action")

        # Attempt to advance to the next view
        current_follow_view = self._follow_view.get_view()
        if not self._follow_view.set_view(current_follow_view + 1):
            self._log.warning(f"View change to {current_follow_view + 1} too soon, not processing request.")
            return web.Response(text="View change attempted too soon.")

        new_leader_id = self._follow_view.get_leader()
        self._log.info(f"Initiating vote for view {self._follow_view.get_view()}. New leader will be {new_leader_id}.")
        self._is_leader = (self._index == new_leader_id) # Update leader status based on new follow_view

        await self._post_view_change_vote()
        return web.Response(text="View change vote initiated.")

    async def receive_view_change_vote(self, request: web.Request):
        json_data = await request.json()
        voter_index = json_data['node_index']
        voted_view_number = json_data['view_number']
        self._log.info(f"Received VIEW_CHANGE_VOTE from node {voter_index} for view {voted_view_number}.")

        if voted_view_number <= self._follow_view.get_view() and voted_view_number not in self._view_change_votes_by_view_number:
             # If it's for an older or current view we are past, but we haven't recorded votes for it,
             # it might be a straggler for a view we already moved on from or are processing.
             # Or, if it IS for the current _follow_view, we process it.
             # The primary concern is not to process votes for views strictly less than our _follow_view
             # unless _follow_view itself hasn't advanced much due to lack of consensus.
             # For simplicity, if it's <= current follow_view and we don't have a vote set, create one.
             # This might need stricter conditions to prevent processing very old votes if _follow_view advanced.
             pass # Allow processing if it's for a view we are currently gathering votes for.

        if voted_view_number < self._follow_view.get_view():
            self._log.warning(f"Ignoring stale VIEW_CHANGE_VOTE for view {voted_view_number} (current follow_view: {self._follow_view.get_view()})")
            return web.Response()


        if voted_view_number not in self._view_change_votes_by_view_number:
            self._view_change_votes_by_view_number[voted_view_number] = (
                ViewChangeVotes(self._index, self._node_cnt))

        # Update our stable checkpoint if the voter's checkpoint is more advanced
        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        self.committed_this_interval_to_blockchain = False # Checkpoint changed, reset


        votes_for_this_view = self._view_change_votes_by_view_number[voted_view_number]
        votes_for_this_view.receive_vote(json_data) # Aggregates checkpoint and prepare certs

        # If this vote pushes us to accept the new view (e.g. we see f+1 votes for it)
        # and we haven't already moved our _follow_view to it or beyond.
        if voted_view_number > self._follow_view.get_view() and len(votes_for_this_view.from_nodes) >= self._f + 1:
            self._log.info(f"Sufficient votes ({len(votes_for_this_view.from_nodes)}) received for view {voted_view_number}, "
                           f"updating follow_view from {self._follow_view.get_view()}.")
            self._follow_view.set_view(voted_view_number) # Advance our follow_view
            self._is_leader = (self._index == self._follow_view.get_leader())
            # If we weren't already voting for this view, cast our vote now that we're convinced.
            # This check ensures a node joins a view change if it sees enough support.
            if self._index not in votes_for_this_view.from_nodes:
                 await self._post_view_change_vote() # Vote for the view we just adopted


        # If we have 2f+1 votes for `voted_view_number`
        if len(votes_for_this_view.from_nodes) >= 2 * self._f + 1:
            self._log.info(f"Have 2f+1 votes for view {voted_view_number}.")
            # And if this node is the designated leader for `voted_view_number`
            # And we are not already leader for this view (or a higher one)
            designated_leader_for_voted_view = voted_view_number % self._node_cnt
            if designated_leader_for_voted_view == self._index and \
               (not self._is_leader or self._view.get_view() < voted_view_number) :

                self._log.info(f"Becoming leader for view {voted_view_number}!")
                self._is_leader = True
                self._view.set_view(voted_view_number) # Our actual operational view becomes this new view
                self._follow_view.set_view(voted_view_number) # Align follow_view too

                # New primary needs to establish the state (P-set from PBFT paper)
                # It uses the aggregated `prepare_certificate_by_slot` and `latest_checkpoint`
                # from the `ViewChangeVotes` object.
                
                # Determine the starting slot for new proposals (or re-proposals)
                # This should be after the latest checkpoint in votes_for_this_view.latest_checkpoint
                if votes_for_this_view.lastest_checkpoint:
                    self._ckpt.update_checkpoint(votes_for_this_view.lastest_checkpoint) # Adopt the best checkpoint
                    self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
                    self.committed_this_interval_to_blockchain = False # Checkpoint changed

                # Determine the highest slot number mentioned in any prepare certificate
                max_prepared_slot = -1
                if votes_for_this_view.prepare_certificate_by_slot:
                    max_prepared_slot = max(
                        [int(s_str) for s_str in votes_for_this_view.prepare_certificate_by_slot.keys()], default=-1
                    )

                self._next_propose_slot = max(self._ckpt.next_slot, max_prepared_slot + 1)

                proposals_to_fill = {}
                # Iterate from the new checkpoint's next_slot up to the max_prepared_slot
                # For any slot in this range:
                #   - If it has a prepare_certificate in votes_for_this_view, and this node hasn't committed it, re-propose it.
                #   - If it does NOT have a prepare_certificate, propose a NO-OP.
                for i in range(self._ckpt.next_slot, self._next_propose_slot): # Up to, but not including, _next_propose_slot
                    slot_str = str(i)
                    if slot_str in self._status_by_slot and self._status_by_slot[slot_str].commit_certificate:
                        self._log.debug(f"ViewChange: Slot {slot_str} already committed by new primary, skipping.")
                        continue # Already committed by this node

                    if slot_str in votes_for_this_view.prepare_certificate_by_slot:
                        # This slot had a prepare certificate from the view change votes
                        # The new primary re-proposes this.
                        cert = votes_for_this_view.prepare_certificate_by_slot[slot_str]
                        proposals_to_fill[slot_str] = cert.get_proposal()
                        self._log.debug(f"ViewChange: New primary will re-propose for slot {slot_str} from old prepare cert.")
                    else:
                        # This slot had no prepare certificate, so propose NO-OP
                        proposals_to_fill[slot_str] = {
                            'id': (-1, -1), # Special ID for NO-OP
                            'client_url': "no_op_url",
                            'timestamp': time.time(), # Timestamp for NO-OP
                            'data': PBFTHandler.NO_OP
                        }
                        self._log.debug(f"ViewChange: New primary will propose NO_OP for slot {slot_str}.")
                
                if proposals_to_fill:
                    await self.fill_bubbles(proposals_to_fill)
                else:
                    self._log.info(f"ViewChange: No bubbles to fill for new primary in view {voted_view_number}.")
                    # If no bubbles, primary can start accepting new client requests for self._next_propose_slot
        return web.Response()

    async def fill_bubbles(self, proposal_by_slot_dict): # Renamed param
        '''
        New primary in a new view sends PRE-PREPARE messages for slots
        that were prepared in previous views or need NO-OPs.
        '''
        if not proposal_by_slot_dict:
            self._log.info("Fill_bubbles called with no proposals.")
            return

        self._log.info("New primary (node %d) in view %d filling bubbles for %d slot(s).",
                       self._index, self._view.get_view(), len(proposal_by_slot_dict))

        # The 'proposal' in a PRE-PREPARE message is a dictionary: {slot_str: client_request_data}
        # We are sending one PRE-PREPARE that might cover multiple slots if the original preprepare did,
        # OR one PRE-PREPARE per slot. The current code structure implies one PRE-PREPARE per slot normally,
        # so let's stick to that for fill_bubbles too for consistency, even if less efficient.
        # However, the original code seems to structure the 'proposal' field in preprepare
        # as a dict {slot: data}. So, `fill_bubbles` can send a single preprepare message
        # with multiple slots if `proposal_by_slot_dict` contains multiple.

        preprepare_msg_for_bubbles = {
            'leader': self._index,
            'view': self._view.get_view(), # Current new view
            'proposal': proposal_by_slot_dict, # This contains {slot_str: data_for_slot, ...}
            'type': 'preprepare'
        }
        self._log.debug("Fill_bubbles PRE-PREPARE content: %s", str(preprepare_msg_for_bubbles)[:200])
        await self._post(self._nodes, PBFTHandler.PREPARE, preprepare_msg_for_bubbles)

    async def garbage_collection(self):
        '''
        Delete those status in self._status_by_slot if its
        slot smaller than next slot of the checkpoint.
        Also tells checkpoint object to GC its votes.
        '''
        while True: # Run periodically
            await asyncio.sleep(self._sync_interval * 2) # Example: GC less often than sync
            self._log.debug("Running garbage collection. Current ckpt.next_slot: %d", self._ckpt.next_slot)
            delete_slots = []
            for slot_str in self._status_by_slot: # Iterate over keys
                if int(slot_str) < self._ckpt.next_slot:
                    delete_slots.append(slot_str)

            for slot_to_delete in delete_slots:
                if slot_to_delete in self._status_by_slot: # Check if still exists
                    del self._status_by_slot[slot_to_delete]
            if delete_slots:
                self._log.info("Garbage collected status for %d slots: %s", len(delete_slots), delete_slots)

            # Garbage collection for checkpoint votes
            await self._ckpt.garbage_collection_votes()


    async def show_blockchain(self, request: web.Request): # Made async and added request param
        # Name param was unused, removed for now
        # text = "show blockchain here "
        self._log.info(f"Node {self._index} received /blockchain request.")
        chain_data = [block.get_json() for block in self._blockchain.chain] # Get JSON of each block
        return web.Response(text=json.dumps(chain_data, indent=4), content_type='application/json')



def logging_config(log_level_str='INFO', log_file=None): # Changed default, accept string
    # Convert string log level to logging level
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # More robust way to get root logger and avoid re-configuring
    if logging.getLogger().hasHandlers() and not getattr(logging_config, 'called_already', False):
        # If handlers exist, but we haven't set them up, perhaps clear them
        # Or, simply assume they are fine if set by basicConfig elsewhere.
        # For now, if called multiple times, it won't add more handlers after the first.
        pass
    elif getattr(logging_config, 'called_already', False):
        return # Already configured by this function

    logging.basicConfig(level=log_level,
                        format="[%(levelname)s]%(name)s->%(funcName)s: \t %(message)s \t --- %(asctime)s",
                        handlers=[logging.StreamHandler(sys.stdout)]) # Ensure it goes to stdout for scripts

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        fh = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        # Formatter from basicConfig is usually inherited, or set one explicitly if needed
        # fh.setFormatter(f) # If 'f' was defined
        fh.setLevel(log_level)
        logging.getLogger().addHandler(fh)
    logging_config.called_already = True


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def arg_parse():
    # parse command line options
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-i', '--index', required=True, type=int, help='node index') # Made index required
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    parser.add_argument('-lf', '--log_to_file', default=False, type=str2bool, help='Whether to dump log messages to file, default = False')
    parser.add_argument('--log_level', default='INFO', type=str, help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    conf = yaml.safe_load(conf_file)
    return conf


async def start_background_tasks(app_obj): # Renamed from 'app' to 'app_obj' to avoid conflict
    '''Helper function to start background tasks'''
    pbft_handler = app_obj['pbft_handler']
    pbft_handler._log.info("Starting background tasks (synchronize, garbage_collection)...")
    app_obj['synchronize_task'] = asyncio.create_task(pbft_handler.synchronize())
    app_obj['garbage_collection_task'] = asyncio.create_task(pbft_handler.garbage_collection())


async def cleanup_background_tasks(app_obj): # Renamed from 'app' to 'app_obj'
    '''Helper function to cleanup background tasks on shutdown'''
    pbft_handler = app_obj['pbft_handler']
    pbft_handler._log.info("Cleaning up background tasks...")

    tasks_to_cancel = {}
    if 'synchronize_task' in app_obj:
        tasks_to_cancel['synchronize_task'] = app_obj['synchronize_task']
    if 'garbage_collection_task' in app_obj:
        tasks_to_cancel['garbage_collection_task'] = app_obj['garbage_collection_task']

    for name, task in tasks_to_cancel.items():
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pbft_handler._log.info(f"{name} task cancelled.")
            except Exception as e:
                pbft_handler._log.error(f"Error during {name} task cleanup: {e}")


def main():
    args = arg_parse()
    log_file_name = f'~$node_{args.index}.log' if args.log_to_file else None
    logging_config(log_level_str=args.log_level, log_file=log_file_name) # Pass log_level from args

    # Get the main logger for the application after basicConfig
    # This logger name will be 'root' if not specified, or can be named.
    log = logging.getLogger(f"Node[{args.index}].Main") # More specific logger
    conf = conf_parse(args.config)
    log.debug("Configuration loaded: %s", conf)

    addr = conf['nodes'][args.index]
    host = addr['host']
    port = addr['port']

    # Create the PBFTHandler instance
    pbft = PBFTHandler(args.index, conf)

    # Create the web application
    app = web.Application()

    # Store the pbft handler in the app context to access it in signal handlers
    app['pbft_handler'] = pbft

    # Add routes
    app.add_routes([
        web.post('/' + PBFTHandler.REQUEST, pbft.get_request),
        web.post('/' + PBFTHandler.PREPREPARE, pbft.prepare), # PREPREPARE messages are handled by pbft.prepare
        web.post('/' + PBFTHandler.PREPARE, pbft.commit),    # PREPARE messages are handled by pbft.commit
        web.post('/' + PBFTHandler.COMMIT, pbft.reply),      # COMMIT messages are handled by pbft.reply
        web.post('/' + PBFTHandler.REPLY, pbft.reply),       # This route seems redundant or a typo if also for COMMITs
        web.post('/' + PBFTHandler.RECEIVE_CKPT_VOTE, pbft.receive_ckpt_vote),
        web.post('/' + PBFTHandler.RECEIVE_SYNC, pbft.receive_sync),
        web.post('/' + PBFTHandler.VIEW_CHANGE_REQUEST, pbft.get_view_change_request),
        web.post('/' + PBFTHandler.VIEW_CHANGE_VOTE, pbft.receive_view_change_vote),
        web.get('/blockchain', pbft.show_blockchain), # Simpler route name
        ])

    # Register startup and cleanup signals
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    log.info(f"Starting node {args.index} on http://{host}:{port}")
    # Run the app
    web.run_app(app, host=host, port=port, access_log=None) # access_log=None to reduce console noise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical("Critical error in main: %s", e, exc_info=True)
        traceback.print_exc() # Ensure traceback is printed for fatal errors