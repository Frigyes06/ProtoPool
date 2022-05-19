import socket
import json

import server
import accountancy
import wallet_json_rpc
from params import wallet_mining_ip, wallet_mining_port
from log_module import logger

last_miner_notify_flag = True
last_miner_notify = ["", "", ""]
last_miner_notify_cnt = 0
last_miner_notify_buf_full = False

last_miner_notify_timeout = 180

buffer = 4096

cli = None

def client_handler():
    """Client handler. Handles miner connections"""
    global last_miner_notify, cli, last_miner_notify_cnt, last_miner_notify_buf_full, last_miner_notify_flag
    wallet_ok = False
    while True:

        print("Client to wallet is starting...")
        try:
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect((wallet_mining_ip, wallet_mining_port))
            wallet_ok = True
            print("Client to wallet started")
        except Exception as e:
            logger.error("WALLET client to wallet cannot be started.")
            print("Client to wallet cannot be started.")
            print(e)
            wallet_ok = False

        while wallet_ok is True:
            try:
                data = cli.recv(buffer)
            except Exception as e:
                logger.error("WALLET receiver buffer error, error: " + str(e))
                wallet_ok = False
                cli.close()
                break

            if not data:
                logger.error("WALLET closed connection")
                cli.close()
                break
            data_str = data.decode("utf-8")
            data_str = data_str.replace('null', '"null"')
            msgs = data_str.split('\n')
            for msg in msgs:
                if msg:
                    try:
                        msg = json.loads(msg)
                    except ValueError:
                        print("Wallet message JSON parsing error")
                        continue
                if "method" in msg and msg["method"] == "miner-notify":
                    last_miner_notify_cnt += 1
                    if last_miner_notify_cnt == 2:
                        last_miner_notify_buf_full = True
                        last_miner_notify_cnt = 0
                    last_miner_notify[last_miner_notify_cnt] = msg
                    accountancy.current_block = msg["params"][0]["block"]
                    server.send_mining_notify_to_all()
                    last_miner_notify_flag = True

                if "result" in msg and "pow" in msg["result"]:
                    print("NEW BLOCK FOUND!! YEEEE  NEW BLOCK FOUND!! YEEEE  NEW BLOCK FOUND!! YEEEE  NEW BLOCK FOUND!! YEEEE  NEW BLOCK FOUND!! YEEEE")
                    accountancy.new_block_accountancy()


def mining_submit_handler(submit_msg, extranonce):
    """Handles the submission of found block to the wallet. server.py references it"""
    global last_miner_notify, last_miner_notify_cnt, cli

    timestamp_dec = str(int(submit_msg["params"][3], 16))
    nonce = str(int(submit_msg["params"][4], 16))
    payload = extranonce + submit_msg["params"][2]
    msg = '{"id": 10, "method": "miner-submit", "params": [{"payload": "' + payload + '","timestamp":' + timestamp_dec + ',"nonce":' + nonce + '}]}\n'

    if wallet_json_rpc.wallet_ok is True:
        cli.sendall(msg.encode())
        print("Block pow sent to wallet:")
        print(msg.encode())
