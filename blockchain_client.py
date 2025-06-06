#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
import json
import asyncio
import aiohttp
from aiohttp import web
import random  # Added for random transactions
import hashlib
import traceback
import sys

# ... (View and Status classes remain the same) ...
class View:
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
    # To encode to json
    def get(self):
        return self._view_number
    # Recover from json data.
    def set_view(self, view):
        self._view_number = view
        self._leader = view % self._num_nodes

class Status:
    def __init__(self, f):
        self.f = f
        self.reply_msgs = {}

    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, view, proposal, from_node):
        # Use sha256 and ensure lines are separate
        hash_object = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get(), hash_object.digest())
        if key not in self.reply_msgs:
            self.reply_msgs[key] = self.SequenceElement(proposal)
        self.reply_msgs[key].from_nodes.add(from_node)

    def _check_succeed(self):
        for key in self.reply_msgs:
            if len(self.reply_msgs[key].from_nodes)>= self.f + 1:
                return True
        return False

# ... (logging_config, arg_parse, conf_parse, make_url functions remain the same) ...

def logging_config(log_level_str='INFO', log_file=None): # Changed default, accept string
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    if logging.getLogger().hasHandlers() and not getattr(logging_config, 'called_already', False):
        pass
    elif getattr(logging_config, 'called_already', False):
        return

    logging.basicConfig(level=log_level,
                        format="[%(levelname)s]%(name)s->%(funcName)s: \t %(message)s \t --- %(asctime)s",
                        handlers=[logging.StreamHandler(sys.stdout)])

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        fh = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        fh.setLevel(log_level)
        logging.getLogger().addHandler(fh)
    logging_config.called_already = True


def arg_parse():
    parser = argparse.ArgumentParser(description='PBFT Client Node') # Changed description slightly
    parser.add_argument('-id', '--client_id', required=True, type=int, help='client id') # Made id required
    parser.add_argument('-nm', '--num_messages', default=1, type=int, help='number of message want to send for this client')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration file [%(default)s]')
    parser.add_argument('--log_level', default='INFO', type=str, help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    conf = yaml.safe_load(conf_file)
    return conf

def make_url(node, command):
    return "http://{}:{}/{}".format(node['host'], node['port'], command)


class Client:
    REQUEST = "request"
    REPLY = "reply"
    VIEW_CHANGE_REQUEST = 'view_change_request'

    def __init__(self, conf, args, client_id_for_log): # Added client_id_for_log
        self._nodes = conf['nodes']
        self._resend_interval = conf['misc']['network_timeout']
        self._client_id = args.client_id
        self._num_messages = args.num_messages
        self._session = None
        self._address = conf['clients'][self._client_id]
        self._client_url = "http://{}:{}".format(self._address['host'],
            self._address['port'])
        self._log = logging.getLogger(f"Client[{client_id_for_log}]") # Use passed id for logger name

        self._retry_times = conf['retry_times_before_view_change']
        self._f = (len(self._nodes) - 1) // 3
        self._is_request_succeed = None
        self._status = None
        self.request_task = None # To hold the task for cancellation

    async def _ensure_session(self): # Added helper
        if not self._session or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._resend_interval)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def request_view_change(self):
        await self._ensure_session()
        json_data = {
            "action" : "view change"
        }
        self._log.info("Broadcasting VIEW_CHANGE_REQUEST to all nodes.")
        tasks = []
        for i in range(len(self._nodes)):
            url = make_url(self._nodes[i], Client.VIEW_CHANGE_REQUEST)
            tasks.append(asyncio.create_task(self._session.post(url, json=json_data)))

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for i, resp_or_exc in enumerate(responses):
            if isinstance(resp_or_exc, Exception):
                self._log.info(f"---> Failed to send view change message to node {i}: {resp_or_exc}")
            else:
                self._log.info(f"---> Sent view change message to node {i}, status: {resp_or_exc.status}")


    async def get_reply(self, request: web.Request):
        json_data = await request.json()
        proposal_ts = json_data.get('proposal', {}).get('timestamp', 0)

        if self._status is None:
            self._log.warning("Received reply but no active request status. Ignoring.")
            return web.Response(status=400, text="No active request")

        if time.time() - proposal_ts >= self._resend_interval * (self._retry_times +1) :
            self._log.warning("Received stale reply. Ignoring.")
            return web.Response(text="Stale reply")

        view = View(json_data['view'], len(self._nodes))
        self._status._update_sequence(view, json_data['proposal'], json_data['index'])

        # Updated logging line with sha256
        hash_digest = hashlib.sha256(json.dumps(json_data['proposal'], sort_keys=True).encode()).digest()
        key = (view.get(), hash_digest)
        from_nodes_count = len(self._status.reply_msgs[key].from_nodes) if key in self._status.reply_msgs else 0
        self._log.debug(f"Received reply from node {json_data['index']} for proposal ts {proposal_ts}. From nodes count: {from_nodes_count}")

        # Use is_set() and _f
        if self._is_request_succeed and not self._is_request_succeed.is_set() and self._status._check_succeed():
            # Use _f
            self._log.info(f"Sufficient replies ({self._f + 1}) received for current request. Setting success event.")
            self._is_request_succeed.set()

        return web.Response(text="Reply processed")


    async def request(self): # This is the main loop for sending requests
        await self._ensure_session()

        for i in range(self._num_messages):
            # Add random delay (e.g., 0.1 to 1.5 seconds)
            await asyncio.sleep(random.uniform(0.1, 1.5))

            # Generate random transaction
            transaction_type = random.choice(['buy', 'sell'])
            random_value = random.randint(10, 100)
            message_content = f"{transaction_type}:{random_value}"

            self._log.info(f"Preparing to send message {i} (client_seq {i}): {message_content}.")
            await self.send_single_request(message_content, i) # Pass the new content
            if self._session and self._session.closed :
                 break


        self._log.info(f"Client {self._client_id} finished sending all {self._num_messages} messages.")
        if self._session and not self._session.closed:
            await self._session.close()
            self._log.info("Client session closed.")


    async def send_single_request(self, message_content, sequence_id):
        accumulate_failure = 0
        is_sent_and_confirmed = False
        current_target_node_idx = 0

        self._is_request_succeed = asyncio.Event()
        self._status = Status(self._f)

        json_data_for_request = {
            'id': (self._client_id, sequence_id),
            'client_url': self._client_url + "/" + Client.REPLY,
            'timestamp': time.time(),
            'data': message_content
        }

        while not is_sent_and_confirmed:
            if self._session and self._session.closed:
                self._log.warning("Session closed, cannot send request.")
                return

            target_node = self._nodes[current_target_node_idx]
            self._log.debug(f"Attempting to send request (seq:{sequence_id}) to node {current_target_node_idx} ({target_node['host']}:{target_node['port']})")
            json_data_for_request['timestamp'] = time.time()

            try:
                await self._ensure_session()
                await self._session.post(make_url(target_node, Client.REQUEST), json=json_data_for_request)
                self._log.debug(f"Request (seq:{sequence_id}) POSTed to node {current_target_node_idx}.")
                await asyncio.wait_for(self._is_request_succeed.wait(), self._resend_interval)
                is_sent_and_confirmed = True
                self._log.info(f"---> Client {self._client_id}'s message (seq:{sequence_id}) confirmed successfully.")
            except asyncio.TimeoutError:
                self._log.info(f"---> Client {self._client_id}'s message (seq:{sequence_id}) to node {current_target_node_idx} timed out waiting for replies.")
                self._is_request_succeed.clear()
                accumulate_failure += 1
                if accumulate_failure >= self._retry_times:
                    self._log.warning(f"Retry limit ({self._retry_times}) reached for message (seq:{sequence_id}). Requesting view change.")
                    await self.request_view_change()
                    await asyncio.sleep(self._resend_interval / 2 + random.random())
                    accumulate_failure = 0
                current_target_node_idx = (current_target_node_idx + 1) % len(self._nodes)
                self._log.info(f"Retrying message (seq:{sequence_id}) with node {current_target_node_idx}.")
            except aiohttp.ClientError as e:
                self._log.error(f"ClientError sending request (seq:{sequence_id}) to node {current_target_node_idx}: {e}")
                self._is_request_succeed.clear()
                accumulate_failure += 1
                if accumulate_failure >= self._retry_times:
                    await self.request_view_change()
                    await asyncio.sleep(self._resend_interval / 2 + random.random())
                    accumulate_failure = 0
                current_target_node_idx = (current_target_node_idx + 1) % len(self._nodes)
            except Exception as e:
                self._log.error(f"Unexpected error for message (seq:{sequence_id}): {e}", exc_info=True)
                break


async def start_client_requests(app_obj):
    client_instance = app_obj['client_instance']
    client_instance._log.info("Starting client request coroutine.")
    app_obj['client_request_task'] = asyncio.create_task(client_instance.request())

async def cleanup_client_tasks(app_obj):
    client_instance = app_obj['client_instance']
    client_instance._log.info("Cleaning up client tasks...")
    if 'client_request_task' in app_obj:
        task = app_obj['client_request_task']
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                client_instance._log.info("Client request task cancelled.")
            except Exception as e:
                client_instance._log.error(f"Error during client_request_task cleanup: {e}")
    if client_instance._session and not client_instance._session.closed:
        await client_instance._session.close()
        client_instance._log.info("Client HTTP session closed during cleanup.")


def setup(args = None):
    if args is None:
        class Args:
            def __init__(self):
                self.client_id      = 0
                self.num_messages   = 1
                self.config         = open('pbft.yaml', 'r')
                self.log_level      = 'INFO'
        args = Args()

    logger_name = f"Client[{args.client_id}]"

    conf = conf_parse(args.config)

    client_instance = Client(conf, args, args.client_id)
    return client_instance


def run_app(client_instance):
    addr = client_instance._address
    host = addr['host']
    port = addr['port']

    app = web.Application()
    app['client_instance'] = client_instance

    app.add_routes([
        web.post('/' + Client.REPLY, client_instance.get_reply),
    ])

    app.on_startup.append(start_client_requests)
    app.on_cleanup.append(cleanup_client_tasks)

    client_instance._log.info(f"Starting client {client_instance._client_id} HTTP server on http://{host}:{port} to listen for replies.")
    client_instance._log.info(f"Will send {client_instance._num_messages} messages.")
    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    parsed_args = arg_parse()
    logging_config(log_level_str=parsed_args.log_level, log_file=f'~$client_{parsed_args.client_id}.log')
    log = logging.getLogger(f"Client[{parsed_args.client_id}]")

    log.info("blockchain_client.py executed directly.")
    client_obj = setup(parsed_args)
    try:
        run_app(client_obj)
    except Exception as e:
        log.critical("Critical error in client main: %s", e, exc_info=True)