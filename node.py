#! /usr/bin/env python3
import logging
import traceback
import argparse
import yaml
import time
from random import random # random.random() is used, ensure import
# from collections import Counter # Not used
import json
import sys
import asyncio
import aiohttp
from aiohttp import web
import hashlib
import heapq

VIEW_SET_INTERVAL = 10

class View: # (Existing code - no changes)
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
        self._min_set_interval = VIEW_SET_INTERVAL
        self._last_set_time = time.time()
    def get_view(self): return self._view_number
    def set_view(self, view):
        if time.time() - self._last_set_time < self._min_set_interval: return False
        self._last_set_time = time.time()
        self._view_number = view
        self._leader = view % self._num_nodes
        return True
    def get_leader(self): return self._leader

class Status: # (Existing code - no changes)
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = "reply"
    def __init__(self, f):
        self.f = f; self.request = 0; self.prepare_msgs = {}; self.prepare_certificate = None
        self.commit_msgs = {}; self.commit_certificate = None; self.is_committed = False
    class Certificate:
        def __init__(self, view, proposal = 0): self._view = view; self._proposal = proposal
        def to_dict(self): return { 'view': self._view.get_view(), 'proposal': self._proposal }
        def dumps_from_dict(self, dictionary): self._view.set_view(dictionary['view']); self._proposal = dictionary['proposal']
        def get_proposal(self): return self._proposal
    class SequenceElement:
        def __init__(self, proposal): self.proposal = proposal; self.from_nodes = set([])
    def _update_sequence(self, msg_type, view, proposal, from_node):
        hash_object = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get_view(), hash_object.digest())
        target_map = self.prepare_msgs if msg_type == Status.PREPARE else self.commit_msgs
        if key not in target_map: target_map[key] = self.SequenceElement(proposal)
        target_map[key].from_nodes.add(from_node)
    def _check_majority(self, msg_type):
        cert, msgs = (self.prepare_certificate, self.prepare_msgs) if msg_type == Status.PREPARE else \
                     (self.commit_certificate, self.commit_msgs)
        if cert: return True
        for key in msgs:
            if len(msgs[key].from_nodes) >= 2 * self.f + 1: return True
        return False

class CheckPoint: # (Existing code - no changes)
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    def __init__(self, interval, nodes, f, idx, loss, net_timeout):
        self._checkpoint_interval=interval; self._nodes=nodes; self._f=f; self._node_index=idx
        self._loss_rate=loss; self._log=logging.getLogger(f"Node[{idx}].CheckPoint")
        self.next_slot=0; self.checkpoint=[]; self._received_votes_by_ckpt={}
        self._session=None; self._network_timeout=net_timeout; self._log.info("---> Create checkpoint.")
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot): self.from_nodes=set([]); self.checkpoint=ckpt; self.next_slot=next_slot
    def get_commit_upperbound(self): return self.next_slot + 2 * self._checkpoint_interval
    def _hash_ckpt(self, ckpt): return hashlib.sha256(json.dumps(ckpt, sort_keys=True).encode()).digest()
    async def _ensure_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._network_timeout))
    async def receive_vote(self, ckpt_vote):
        ckpt=json.loads(ckpt_vote['ckpt']); next_s=ckpt_vote['next_slot']; from_n=ckpt_vote['node_index']
        h_ckpt=self._hash_ckpt(ckpt)
        if h_ckpt not in self._received_votes_by_ckpt:
            self._received_votes_by_ckpt[h_ckpt] = CheckPoint.ReceiveVotes(ckpt, next_s)
        self._received_votes_by_ckpt[h_ckpt].from_nodes.add(from_n)
        updated=False
        for current_h in list(self._received_votes_by_ckpt.keys()):
            if current_h in self._received_votes_by_ckpt:
                v_stat=self._received_votes_by_ckpt[current_h]
                if v_stat.next_slot > self.next_slot and len(v_stat.from_nodes) >= 2*self._f+1:
                    self.next_slot=v_stat.next_slot; self.checkpoint=v_stat.checkpoint; updated=True
                    self._log.info(f"Update checkpoint by votes. New next_slot: {self.next_slot}")
        if updated: await self.garbage_collection_votes()
    async def propose_vote(self, decisions): await self._broadcast_checkpoint(self.checkpoint+decisions, 'vote', CheckPoint.RECEIVE_CKPT_VOTE)
    async def _post(self, nodes_l, cmd, data):
        await self._ensure_session()
        for i, n_info in enumerate(nodes_l):
            if random() > self._loss_rate:
                try: await self._session.post(self.make_url(n_info, cmd), json=data)
                except Exception as e: self._log.error(f"Post to {i} cmd {cmd} failed: {e}")
    @staticmethod
    def make_url(n, c): return f"http://{n['host']}:{n['port']}/{c}"
    async def _broadcast_checkpoint(self, ckpt, type, cmd):
        await self._post(self._nodes, cmd, {'node_index':self._node_index, 'next_slot':self.next_slot+self._checkpoint_interval, 'ckpt':json.dumps(ckpt), 'type':type})
    def get_ckpt_info(self): return {'next_slot':self.next_slot, 'ckpt':json.dumps(self.checkpoint)}
    def update_checkpoint(self, data):
        if data['next_slot'] > self.next_slot:
            self.next_slot=data['next_slot']; self.checkpoint=json.loads(data['ckpt'])
            self._log.info(f"Update checkpoint by sync. New next_slot: {self.next_slot}")
    async def garbage_collection_votes(self):
        dels=[h for h,v in self._received_votes_by_ckpt.items() if v.next_slot <= self.next_slot]
        for h_del in dels:
            if h_del in self._received_votes_by_ckpt: del self._received_votes_by_ckpt[h_del]
        if dels: self._log.debug(f"GC {len(dels)} ckpt vote entries.")

class ViewChangeVotes: # (Existing code - no changes)
    def __init__(self, node_idx, num_nodes):
        self._node_index=node_idx; self._num_total_nodes=num_nodes; self._f=(num_nodes-1)//3
        self.from_nodes=set([]); self.prepare_certificate_by_slot={}; self.lastest_checkpoint=None
        self._log=logging.getLogger(f"Node[{node_idx}].ViewChangeVotes")
    def receive_vote(self, json_data):
        prep_certs=json_data["prepare_certificates"]
        for slot_s, cert_d in prep_certs.items():
            prep_cert=Status.Certificate(View(0,self._num_total_nodes)); prep_cert.dumps_from_dict(cert_d)
            if slot_s not in self.prepare_certificate_by_slot or \
               self.prepare_certificate_by_slot[slot_s]._view.get_view() < prep_cert._view.get_view():
                self.prepare_certificate_by_slot[slot_s]=prep_cert
        voter_ckpt_info=json_data["checkpoint"]
        if self.lastest_checkpoint is None or voter_ckpt_info['next_slot'] > self.lastest_checkpoint['next_slot']:
            self.lastest_checkpoint=voter_ckpt_info
        self.from_nodes.add(json_data['node_index'])

class Block: # (Existing code - timestamp is deterministic)
    def __init__(self, index, transactions, timestamp, previous_hash):
        self.index=index; self.transactions=transactions; self.timestamp=timestamp
        self.previous_hash=previous_hash; self.hash=self.compute_hash() # Compute hash on init
    def compute_hash(self):
        return hashlib.sha256(json.dumps(self.__dict__, sort_keys=True, separators=(',', ':')).encode()).hexdigest() # Added separators for compactness
    def get_json(self):
        return json.dumps(self.__dict__, indent=4, sort_keys=True)

class Blockchain: # (Existing code - deterministic genesis)
    def __init__(self):
        self.commit_counter=0; self.length=0; self.chain=[]; self.create_genesis_block()
    def create_genesis_block(self):
        gb = Block(0, ["Genenesis Block"], 0, "0"); self.length+=1; self.chain.append(gb) # hash computed in Block init
    @property
    def last_block(self): return self.chain[-1]
    def last_block_hash(self): return self.last_block.hash
    def update_commit_counter(self): self.commit_counter = self.length
    def add_block(self, block): # block.hash is pre-computed
        if self.last_block_hash() != block.previous_hash:
            logging.error(f"Block prev_hash mismatch: expected {self.last_block_hash()}, got {block.previous_hash}")
            return
        self.length+=1; self.chain.append(block)


class PBFTHandler:
    REQUEST = 'request'; PREPREPARE = 'preprepare'; PREPARE = 'prepare'; COMMIT = 'commit'
    NO_OP = 'NOP'; RECEIVE_SYNC = 'receive_sync'; RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    VIEW_CHANGE_REQUEST = 'view_change_request'; VIEW_CHANGE_VOTE = "view_change_vote"

    # *** NEW: Threshold for forcing block formation ***
    FORCE_BLOCK_THRESHOLD = 10 # Number of transactions received to trigger forced block

    def __init__(self, index, conf):
        # ... (other initializations remain the same) ...
        self._nodes = conf['nodes']; self._node_cnt = len(self._nodes); self._index = index
        self._log = logging.getLogger(f"Node[{self._index}]"); self._f = (self._node_cnt - 1) // 3
        self._view = View(0, self._node_cnt); self._next_propose_slot = 0
        self._blockchain = Blockchain(); self.committed_this_interval_to_blockchain = True # Start as true, reset when new interval starts
        self._is_leader = (self._index == self._view.get_leader()); self._loss_rate = conf['loss%'] / 100
        self._network_timeout = conf['misc']['network_timeout']; self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes, self._f, self._index, self._loss_rate, self._network_timeout)
        self._last_commit_slot = -1; self._follow_view = View(0, self._node_cnt)
        self._view_change_votes_by_view_number = {}; self._status_by_slot = {}
        self._sync_interval = conf['sync_interval']; self._session = None

        self.buy_max_heap = []  # Stores (-price, timestamp, original_request_data)
        self.sell_max_heap = [] # Stores (-price, timestamp, original_request_data)
        self._log.info("Initialized buy and sell MAX-transaction heaps.")

        # *** NEW: Counter for transactions received since last block was formed by matching/forcing ***
        self.tx_received_since_last_block_attempt = 0


    @staticmethod
    def make_url(node, command): return f"http://{node['host']}:{node['port']}/{command}"
    async def _ensure_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._network_timeout))
    async def _post(self, nodes_list, command, json_data_payload): # (As previously corrected)
        await self._ensure_session()
        tasks = []
        for i, node_info in enumerate(nodes_list):
            if random() > self._loss_rate:
                url = self.make_url(node_info, command)
                self._log.debug("POST to node %d (%s), cmd: %s, data: %.100s", i, url, command, str(json_data_payload))
                tasks.append(asyncio.create_task(self._session.post(url, json=json_data_payload)))
            else:
                self._log.debug("Simulating message loss to node %d for command %s", i, command)
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for i, resp_or_exc in enumerate(responses):
            if isinstance(resp_or_exc, Exception):
                self._log.error(f"Error posting command {command} to a node: {resp_or_exc}")

    def _legal_slot(self, slot_str): # (Existing - simplified for brevity)
        return self._ckpt.next_slot <= int(slot_str) < self._ckpt.get_commit_upperbound()

    async def preprepare(self, client_request_data): # (Existing - takes single tx)
        this_slot_int = self._next_propose_slot
        this_slot_str = str(this_slot_int)
        self._next_propose_slot = this_slot_int + 1
        self._log.info("Preprepare: slot %s, data: %.100s", this_slot_str, str(client_request_data))
        if this_slot_str not in self._status_by_slot:
            self._status_by_slot[this_slot_str] = Status(self._f)
        self._status_by_slot[this_slot_str].request = client_request_data
        msg = {'leader': self._index, 'view': self._view.get_view(),
               'proposal': {this_slot_str: client_request_data}, 'type': PBFTHandler.PREPREPARE}
        await self._post(self._nodes, PBFTHandler.PREPREPARE, msg)

    async def _force_propose_transactions(self):
        """Pops transactions from heaps and proposes them to fill a block."""
        self._log.info(f"Forcing block formation. Attempting to propose up to {self._checkpoint_interval} transactions.")
        proposed_count = 0
        # Alternatingly try to propose from buy and sell heaps to maintain some fairness
        # Or simply prioritize, e.g. largest heap, or buys then sells.
        # For simplicity, let's alternate if possible, prioritizing buys slightly if counts are equal.
        
        while proposed_count < self._checkpoint_interval:
            tx_to_propose_tuple = None
            heap_choice = ""

            # Prioritize popping from the heap with more items, or buy heap if equal and non-empty
            can_pop_buy = bool(self.buy_max_heap)
            can_pop_sell = bool(self.sell_max_heap)

            if not can_pop_buy and not can_pop_sell:
                self._log.info("Heaps are empty, cannot force propose more transactions.")
                break

            if proposed_count % 2 == 0: # Alternate: try buy first on even counts
                if can_pop_buy:
                    tx_to_propose_tuple = heapq.heappop(self.buy_max_heap)
                    heap_choice = "BUY"
                elif can_pop_sell:
                    tx_to_propose_tuple = heapq.heappop(self.sell_max_heap)
                    heap_choice = "SELL"
            else: # Try sell first on odd counts
                if can_pop_sell:
                    tx_to_propose_tuple = heapq.heappop(self.sell_max_heap)
                    heap_choice = "SELL"
                elif can_pop_buy:
                    tx_to_propose_tuple = heapq.heappop(self.buy_max_heap)
                    heap_choice = "BUY"
            
            if tx_to_propose_tuple:
                original_request_data = tx_to_propose_tuple[2]
                price = -tx_to_propose_tuple[0]
                self._log.info(f"Force proposing {heap_choice} transaction (Price: {price}): {original_request_data.get('data')}")
                await self.preprepare(original_request_data)
                proposed_count += 1
            else: # Should not happen if can_pop_buy or can_pop_sell was true
                break
        
        self._log.info(f"Finished force proposing {proposed_count} transactions.")
        # Reset the counter as an attempt to form a block has been made
        self.tx_received_since_last_block_attempt = 0


    async def _try_match_and_maybe_force_block(self):
        if not self._is_leader:
            return

        # Try matching first
        if self.buy_max_heap and self.sell_max_heap:
            max_buy_price_tuple = self.buy_max_heap[0]
            max_sell_price_tuple = self.sell_max_heap[0]
            actual_max_buy_price = -max_buy_price_tuple[0]
            actual_max_sell_price = -max_sell_price_tuple[0]

            if actual_max_buy_price >= actual_max_sell_price:
                # self._log.info(f"Match condition met! Max Buy ({actual_max_buy_price}) >= Max Sell ({actual_max_sell_price}).") # Optional log
                sell_tx_tuple = heapq.heappop(self.sell_max_heap)
                buy_tx_tuple = heapq.heappop(self.buy_max_heap)
                self._log.info(f"Popped Sell (Price: {-sell_tx_tuple[0]}), Buy (Price: {-buy_tx_tuple[0]}) for sequential proposal.")
                
                await self.preprepare(sell_tx_tuple[2]) # Propose sell
                await self.preprepare(buy_tx_tuple[2])  # Propose buy

                self.tx_received_since_last_block_attempt = 0 # Reset counter after successful match leading to proposals
                asyncio.create_task(self._try_match_and_maybe_force_block()) # Check for more matches
                return # Done for this attempt, matching took precedence

        # If no match, check if forcing is needed
        # "if the primary has not formed a block When receiving 20 transactions"
        # This implies if 20 transactions accumulate and no block-forming event (match) happened for them.
        # `committed_this_interval_to_blockchain` tells if the *current* interval is still open.
        # If a block was just formed, this counter `tx_received_since_last_block_attempt` will be low.
        if not self.committed_this_interval_to_blockchain and self.tx_received_since_last_block_attempt >= PBFTHandler.FORCE_BLOCK_THRESHOLD:
            self._log.info(f"No match, but {self.tx_received_since_last_block_attempt} tx received since last block attempt. Threshold ({PBFTHandler.FORCE_BLOCK_THRESHOLD}) met.")
            await self._force_propose_transactions()
            # self.tx_received_since_last_block_attempt is reset inside _force_propose_transactions
        else:
            if not (self.buy_max_heap and self.sell_max_heap):
                 self._log.debug("Not enough buy/sell orders in heaps to attempt a match.")
            else: # Heaps have orders, but no match and threshold not met
                 self._log.debug(f"No match for MaxBuy({-self.buy_max_heap[0][0]}) vs MaxSell({-self.sell_max_heap[0][0]}). "
                                f"Tx since last block attempt: {self.tx_received_since_last_block_attempt}")


    async def get_request(self, request: web.Request):
        self._is_leader = (self._index == self._view.get_leader())
        if not self._is_leader:
            # ... (redirect logic as before) ...
            current_leader_id = self._view.get_leader()
            if current_leader_id is not None and 0 <= current_leader_id < self._node_cnt:
                leader_url = self.make_url(self._nodes[current_leader_id], PBFTHandler.REQUEST)
                raise web.HTTPTemporaryRedirect(location=leader_url)
            raise web.HTTPServiceUnavailable(reason="Current leader unknown or invalid.")
        else: # Leader
            client_req_json = await request.json()
            self._log.info("Leader received: %.100s", client_req_json)
            try:
                data_str = client_req_json.get('data', '')
                tx_type, price_str = data_str.split(':')
                price = int(price_str)
                ts = client_req_json.get('timestamp', time.time())

                if tx_type.lower() == 'buy':
                    heapq.heappush(self.buy_max_heap, (-price, ts, client_req_json))
                    self._log.info(f"BUY heap size: {len(self.buy_max_heap)}, Added: Price={price}")
                elif tx_type.lower() == 'sell':
                    heapq.heappush(self.sell_max_heap, (-price, ts, client_req_json))
                    self._log.info(f"SELL heap size: {len(self.sell_max_heap)}, Added: Price={price}")
                else:
                    return web.Response(text="Unknown transaction type", status=400)
                
                self.tx_received_since_last_block_attempt += 1
                if self.committed_this_interval_to_blockchain: # If a block was just formed, this is the start of a new potential block
                    self.committed_this_interval_to_blockchain = False # Open the interval
                    # self.tx_received_since_last_block_attempt = 1 # Reset and count this one (already done by +=1)

                await self._try_match_and_maybe_force_block()
                return web.Response(text="Request received by leader.")
            except ValueError:
                return web.Response(text="Malformed transaction data", status=400)
            except Exception as e:
                self._log.error(f"Error processing leader request: {e}", exc_info=True)
                return web.Response(text="Error processing request", status=500)

    async def prepare(self, request: web.Request): # (Existing - handles PREPREPARE)
        json_data = await request.json(); sender_view = json_data['view']
        leader_id = json_data['leader']; proposal_dict = json_data['proposal']
        self._log.info("Received PREPREPARE from %d for view %d", leader_id, sender_view)
        for slot_str, prop_data in proposal_dict.items():
            if not self._legal_slot(slot_str): continue
            if slot_str not in self._status_by_slot: self._status_by_slot[slot_str] = Status(self._f)
            if not self._status_by_slot[slot_str].request: self._status_by_slot[slot_str].request = prop_data
            msg = {'index':self._index, 'view':sender_view, 'proposal':{slot_str:prop_data}, 'type':Status.PREPARE}
            await self._post(self._nodes, PBFTHandler.PREPARE, msg) # POST to /prepare (commit handler)
        return web.Response()

    async def commit(self, request: web.Request): # (Existing - handles PREPARE)
        json_data = await request.json(); sender_idx = json_data['index']; sender_view = json_data['view']
        proposal_dict = json_data['proposal']
        self._log.info("Received PREPARE from %d for view %d", sender_idx, sender_view)
        for slot_str, prop_data in proposal_dict.items():
            if not self._legal_slot(slot_str): continue
            if slot_str not in self._status_by_slot: self._status_by_slot[slot_str] = Status(self._f)
            status = self._status_by_slot[slot_str]
            if not status.request: status.request = prop_data
            status._update_sequence(Status.PREPARE, View(sender_view, self._node_cnt), prop_data, sender_idx)
            if not status.prepare_certificate and status._check_majority(Status.PREPARE):
                status.prepare_certificate = Status.Certificate(View(sender_view, self._node_cnt), prop_data)
                msg = {'index':self._index, 'view':sender_view, 'proposal':{slot_str:prop_data}, 'type':Status.COMMIT}
                await self._post(self._nodes, PBFTHandler.COMMIT, msg) # POST to /commit (reply handler)
        return web.Response()

    async def reply(self, request: web.Request): # (Existing - handles COMMIT)
        json_data = await request.json(); sender_idx = json_data['index']; sender_view = json_data['view']
        proposal_dict = json_data['proposal']
        self._log.info("Received COMMIT from %d for view %d", sender_idx, sender_view)
        for slot_str, prop_data in proposal_dict.items():
            if not self._legal_slot(slot_str): continue
            if slot_str not in self._status_by_slot: self._status_by_slot[slot_str] = Status(self._f)
            status = self._status_by_slot[slot_str]
            if not status.request: status.request = prop_data
            status._update_sequence(Status.COMMIT, View(sender_view, self._node_cnt), prop_data, sender_idx)
            if not status.commit_certificate and status._check_majority(Status.COMMIT):
                status.commit_certificate = Status.Certificate(View(sender_view, self._node_cnt), prop_data)
                await self._try_local_commit_and_reply()
        return web.Response()

    async def _try_local_commit_and_reply(self): # (Existing - replies for single tx)
        while True:
            next_slot_str = str(self._last_commit_slot + 1)
            if next_slot_str in self._status_by_slot:
                status = self._status_by_slot[next_slot_str]
                if status.commit_certificate and not status.is_committed:
                    self._log.info("Locally committing slot %s", next_slot_str)
                    status.is_committed = True; self._last_commit_slot += 1
                    client_req = status.commit_certificate.get_proposal()
                    reply_msg = {'index':self._index, 'view':status.commit_certificate._view.get_view(),
                                 'proposal':client_req, 'type':Status.REPLY}
                    if client_req.get('client_url'):
                        try:
                            await self._ensure_session()
                            await self._session.post(client_req['client_url'], json=reply_msg)
                            self._log.info(f"Replied to client for slot {next_slot_str} (id: {client_req.get('id')})")
                        except Exception as e: self._log.error(f"Reply to client for slot {next_slot_str} failed: {e}")
                    
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        self._log.info("Checkpoint interval reached at slot %d.", self._last_commit_slot)
                        decisions = self.get_commit_decisions_for_checkpoint()
                        await self._ckpt.propose_vote(decisions)
                        self.committed_this_interval_to_blockchain = False # Ready for a new set of tx for next block
                        await self._commit_action_to_blockchain()
                else: break
            else: break

    def get_commit_decisions_for_checkpoint(self): # (Existing - stores (id_str, data_str))
        decisions = []
        for i in range(self._ckpt.next_slot, self._last_commit_slot + 1):
            slot_s = str(i)
            if slot_s in self._status_by_slot and self._status_by_slot[slot_s].commit_certificate and \
               self._status_by_slot[slot_s].is_committed:
                prop = self._status_by_slot[slot_s].commit_certificate.get_proposal()
                decisions.append((str(prop.get('id', 'N/A_ID')), prop.get('data', 'N/A_DATA')))
        return decisions

    async def _commit_action_to_blockchain(self): # (Existing - uses deterministic timestamp)
        if self.committed_this_interval_to_blockchain and self._blockchain.length > 1 : # Check length to allow first block
            self._log.debug("Block interval already processed or no new interval started.")
            return

        # Transactions for the current block are those from the completed checkpoint interval
        # These are already formatted by get_commit_decisions_for_checkpoint
        # This method uses ckpt.next_slot and last_commit_slot to define the interval of slots to make into a block.
        # However, the actual transactions are already gathered if propose_vote was called with them.
        # Let's re-gather based on slots for clarity here.
        
        tx_for_block = []
        # The slots for the block are from the *previous* ckpt.next_slot up to current ckpt.next_slot - 1
        # Or, more simply, from _last_commit_slot - _checkpoint_interval + 1 to _last_commit_slot
        start_slot = self._last_commit_slot - self._checkpoint_interval + 1
        end_slot = self._last_commit_slot

        if start_slot < 0 : start_slot = 0 # case for first few blocks if interval not fully met

        for i in range(start_slot, end_slot + 1):
            slot_str = str(i)
            if slot_str in self._status_by_slot and \
               self._status_by_slot[slot_str].is_committed and \
               self._status_by_slot[slot_str].commit_certificate:
                proposal = self._status_by_slot[slot_str].commit_certificate.get_proposal()
                tx_for_block.append((str(proposal.get('id', 'N/A_ID')), proposal.get('data', 'N/A_DATA')))
            # It's possible that a NO-OP was committed, or this slot was part of a previous block
            # This gathering logic might need to be more robust or rely on get_commit_decisions_for_checkpoint directly
            # For now, this aims to get transactions for the just completed interval.

        if not tx_for_block and self._blockchain.length > 1: # Don't add empty blocks unless it's not genesis
            self._log.info("No transactions for current interval to add to blockchain.")
            self.committed_this_interval_to_blockchain = True # Mark interval as "processed"
            self.tx_received_since_last_block_attempt = 0 # Reset counter
            return

        self._log.info(f"Adding new block with {len(tx_for_block)} txs.")
        try:
            timestamp_val = self._blockchain.length # Deterministic timestamp
            new_block = Block(self._blockchain.length, tx_for_block, timestamp_val, self._blockchain.last_block_hash())
            # add_block already computes hash
            self._blockchain.add_block(new_block)
            
            self.committed_this_interval_to_blockchain = True # Mark interval as processed
            self.tx_received_since_last_block_attempt = 0 # Reset counter

            with open(f"~$node_{self._index}.blockchain", 'a') as f:
                for i in range(self._blockchain.commit_counter, self._blockchain.length):
                    f.write(self._blockchain.chain[i].get_json() + '\n------------\n')
                self._blockchain.update_commit_counter()
            self._log.info(f"Written blocks up to index {self._blockchain.length -1} to file.")
        except Exception as e:
            self._log.error("Error in _commit_action_to_blockchain: %s", e, exc_info=True)


    # ... (receive_ckpt_vote, receive_sync, synchronize - existing code)
    # ... (View Change methods - existing code, should handle single tx proposals)
    # ... (garbage_collection, show_blockchain - existing code)

    async def receive_ckpt_vote(self, request: web.Request): # (Existing)
        json_data = await request.json(); await self._ckpt.receive_vote(json_data); return web.Response()
    async def receive_sync(self, request: web.Request): # (Existing)
        json_data = await request.json(); self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        self.committed_this_interval_to_blockchain = False # State might change significantly
        certs = json_data['commit_certificates']
        for slot_s, cert_d in certs.items():
            slot_i = int(slot_s)
            if not (self._ckpt.next_slot <= slot_i < self._ckpt.get_commit_upperbound()): continue
            if slot_s not in self._status_by_slot: self._status_by_slot[slot_s] = Status(self._f)
            stat = self._status_by_slot[slot_s]
            if not stat.commit_certificate:
                cert = Status.Certificate(View(0,self._node_cnt)); cert.dumps_from_dict(cert_d)
                stat.commit_certificate = cert; stat.request = cert.get_proposal()
        await self._try_local_commit_and_reply()
        if (self._last_commit_slot+1)%self._checkpoint_interval==0 and self._last_commit_slot >=0 and \
           not self.committed_this_interval_to_blockchain:
            await self._commit_action_to_blockchain()
        return web.Response()
    async def synchronize(self): # (Existing)
        while True:
            await asyncio.sleep(self._sync_interval); self._log.debug("Periodic synchronize.")
            certs_to_send = {}
            for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
                slot_s = str(i)
                if slot_s in self._status_by_slot and self._status_by_slot[slot_s].commit_certificate:
                    certs_to_send[slot_s] = self._status_by_slot[slot_s].commit_certificate.to_dict()
            data = {'node_index':self._index, 'checkpoint':self._ckpt.get_ckpt_info(), 'commit_certificates':certs_to_send}
            await self._post(self._nodes, PBFTHandler.RECEIVE_SYNC, data)
    async def get_prepare_certificates_for_view_change(self): # (Existing)
        certs = {}
        for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
            slot_s = str(i)
            if slot_s in self._status_by_slot and self._status_by_slot[slot_s].prepare_certificate:
                certs[slot_s] = self._status_by_slot[slot_s].prepare_certificate.to_dict()
        return certs
    async def _post_view_change_vote(self): # (Existing)
        certs = await self.get_prepare_certificates_for_view_change()
        msg = {"node_index":self._index, "view_number":self._follow_view.get_view(),
               "checkpoint":self._ckpt.get_ckpt_info(), "prepare_certificates":certs}
        await self._post(self._nodes, PBFTHandler.VIEW_CHANGE_VOTE, msg)
    async def get_view_change_request(self, request: web.Request): # (Existing)
        json_data = await request.json()
        if json_data.get('action') != "view change": return web.Response(status=400)
        curr_view = self._follow_view.get_view()
        if not self._follow_view.set_view(curr_view + 1):
            return web.Response(text="View change too soon.")
        self._is_leader = (self._index == self._follow_view.get_leader())
        await self._post_view_change_vote(); return web.Response(text="View change vote initiated.")
    async def receive_view_change_vote(self, request: web.Request): # (Existing)
        json_data = await request.json(); voter_idx=json_data['node_index']; voted_view=json_data['view_number']
        if voted_view < self._follow_view.get_view(): return web.Response()
        if voted_view not in self._view_change_votes_by_view_number:
            self._view_change_votes_by_view_number[voted_view] = ViewChangeVotes(self._index, self._node_cnt)
        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        self.committed_this_interval_to_blockchain = False
        votes_this_view = self._view_change_votes_by_view_number[voted_view]
        votes_this_view.receive_vote(json_data)
        if voted_view > self._follow_view.get_view() and len(votes_this_view.from_nodes) >= self._f + 1:
            self._follow_view.set_view(voted_view); self._is_leader=(self._index==self._follow_view.get_leader())
            if self._index not in votes_this_view.from_nodes: await self._post_view_change_vote()
        if len(votes_this_view.from_nodes) >= 2 * self._f + 1:
            new_leader = voted_view % self._node_cnt
            if new_leader == self._index and (not self._is_leader or self._view.get_view() < voted_view):
                self._is_leader=True; self._view.set_view(voted_view); self._follow_view.set_view(voted_view)
                if votes_this_view.lastest_checkpoint:
                    self._ckpt.update_checkpoint(votes_this_view.lastest_checkpoint)
                    self._last_commit_slot=max(self._last_commit_slot, self._ckpt.next_slot-1)
                    self.committed_this_interval_to_blockchain=False
                max_prep_slot = max([int(s) for s in votes_this_view.prepare_certificate_by_slot.keys()] or [-1])
                self._next_propose_slot = max(self._ckpt.next_slot, max_prep_slot + 1)
                proposals_fill = {}
                for i in range(self._ckpt.next_slot, self._next_propose_slot):
                    slot_s=str(i)
                    if slot_s in self._status_by_slot and self._status_by_slot[slot_s].commit_certificate: continue
                    if slot_s in votes_this_view.prepare_certificate_by_slot:
                        proposals_fill[slot_s] = votes_this_view.prepare_certificate_by_slot[slot_s].get_proposal()
                    else: # NO-OP
                        proposals_fill[slot_s] = {'id':(-1,-1), 'client_url':'no_op', 'timestamp':time.time(), 'data':PBFTHandler.NO_OP}
                if proposals_fill: await self.fill_bubbles_unified_proposal(proposals_fill)
        return web.Response()
    async def fill_bubbles_unified_proposal(self, proposal_by_slot_dict): # (Existing)
        if not proposal_by_slot_dict: return
        msg = {'leader':self._index, 'view':self._view.get_view(), 'proposal':proposal_by_slot_dict, 'type':PBFTHandler.PREPREPARE}
        await self._post(self._nodes, PBFTHandler.PREPREPARE, msg)
    async def garbage_collection(self): # (Existing)
        while True:
            await asyncio.sleep(self._sync_interval * 2)
            slots_del = [s for s in self._status_by_slot if int(s) < self._ckpt.next_slot]
            for s_del in slots_del:
                if s_del in self._status_by_slot: del self._status_by_slot[s_del]
            if slots_del: self._log.info(f"GC status for {len(slots_del)} slots.")
            await self._ckpt.garbage_collection_votes()
    async def show_blockchain(self, request: web.Request): # (Existing)
        chain_data = [json.loads(b.get_json()) for b in self._blockchain.chain]
        return web.Response(text=json.dumps(chain_data, indent=4), content_type='application/json')

def logging_config(log_level_str='INFO', log_file=None): # (Existing code)
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    if logging.getLogger().hasHandlers() and not getattr(logging_config, 'called_already', False): pass
    elif getattr(logging_config, 'called_already', False): return
    logging.basicConfig(level=log_level, format="[%(levelname)s]%(name)s->%(funcName)s: \t %(message)s \t --- %(asctime)s", handlers=[logging.StreamHandler(sys.stdout)])
    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        fh = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        fh.setLevel(log_level); logging.getLogger().addHandler(fh)
    logging_config.called_already = True

def str2bool(v): # (Existing code)
    if isinstance(v, bool): return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'): return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'): return False
    else: raise argparse.ArgumentTypeError('Boolean value expected.')

def arg_parse(): # (Existing code)
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-i', '--index', required=True, type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    parser.add_argument('-lf', '--log_to_file', default=False, type=str2bool, help='Whether to dump log messages to file, default = False')
    parser.add_argument('--log_level', default='INFO', type=str, help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    return parser.parse_args()

def conf_parse(conf_file) -> dict: return yaml.safe_load(conf_file) # (Existing code)

async def start_background_tasks(app_obj): # (Existing code)
    pbft_handler = app_obj['pbft_handler']
    pbft_handler._log.info("Starting background tasks (synchronize, garbage_collection)...")
    app_obj['synchronize_task'] = asyncio.create_task(pbft_handler.synchronize())
    app_obj['garbage_collection_task'] = asyncio.create_task(pbft_handler.garbage_collection())

async def cleanup_background_tasks(app_obj): # (Existing code)
    pbft_handler = app_obj['pbft_handler']
    pbft_handler._log.info("Cleaning up background tasks...")
    tasks = {'sync': app_obj.get('synchronize_task'), 'gc': app_obj.get('garbage_collection_task')}
    for name, task in tasks.items():
        if task and not task.done():
            task.cancel()
            try: await task
            except asyncio.CancelledError: pbft_handler._log.info(f"{name} task cancelled.")
            except Exception as e: pbft_handler._log.error(f"Error during {name} task cleanup: {e}")

def main(): # (Existing code - ensure routes are correct)
    args = arg_parse()
    log_file = f'~$node_{args.index}.log' if args.log_to_file else None
    logging_config(log_level_str=args.log_level, log_file=log_file)
    log = logging.getLogger(f"Node[{args.index}].Main")
    conf = conf_parse(args.config)
    log.debug("Config: %s", conf)
    addr = conf['nodes'][args.index]; host = addr['host']; port = addr['port']
    pbft = PBFTHandler(args.index, conf)
    app = web.Application(); app['pbft_handler'] = pbft
    app.add_routes([
        web.post('/' + PBFTHandler.REQUEST, pbft.get_request),
        web.post('/' + PBFTHandler.PREPREPARE, pbft.prepare),       # Endpoint for PREPREPARE from leader
        web.post('/' + PBFTHandler.PREPARE, pbft.commit),         # Endpoint for PREPARE from replicas
        web.post('/' + PBFTHandler.COMMIT, pbft.reply),           # Endpoint for COMMIT from replicas
        web.post('/' + PBFTHandler.RECEIVE_CKPT_VOTE, pbft.receive_ckpt_vote),
        web.post('/' + PBFTHandler.RECEIVE_SYNC, pbft.receive_sync),
        web.post('/' + PBFTHandler.VIEW_CHANGE_REQUEST, pbft.get_view_change_request),
        web.post('/' + PBFTHandler.VIEW_CHANGE_VOTE, pbft.receive_view_change_vote),
        web.get('/blockchain', pbft.show_blockchain),
    ])
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    log.info(f"Starting node {args.index} on http://{host}:{port}")
    web.run_app(app, host=host, port=port, access_log=None)

if __name__ == "__main__":
    try: main()
    except Exception as e: logging.critical("Main error: %s", e, exc_info=True); traceback.print_exc()