import threading
import time

import mining
import wallet_json_rpc
import sqlite_handler
from params import pplns_interval, pool_account, pool_fee, payment_fee, payment_fee_to_pool, payment_prec, orphan_age_limit
from log_module import logger

payment_batches = []
account_fees = {}
current_block = 0

class Payment_batch():
    """
    Payment batch class:

    self.block = The block for which the batch is for
    self.from_account = Sender account, usually pool account
    self.payments = list of payments to be executed in the batch
    self.paid = boolean indicating if the batch is paid or not
    """

    def __init__(self, block, from_account):
        self.block = block
        self.from_account = from_account
        self.payments = {}
        self.paid = False

    def add_payment(self, account, share_rate):
        """Adds payment to payments list"""
        self.payments[account] = share_rate


def new_block_accountancy():
    """new block accountancy. Called in clinet.py when a new block is found"""
    global current_block
    # TODO try-except

    if not sqlite_handler.db.is_block_in_db_already(current_block):
        calc_shares()
        calc_share_rates(current_block, current_block*5)


def calc_shares():
    """Calculate shares"""
    current_time = time.time()
    mining.shares_of_current_block = 0
    for account in mining.shares:
        account_shares = 0
        for timestamp in list(mining.shares[account].timestamps):
            if timestamp >= current_time - pplns_interval:
                account_shares += mining.shares[account].timestamps[timestamp]
            else:
                del mining.shares[account].timestamps[timestamp]
        mining.miners[account] = account_shares
        mining.shares_of_current_block += account_shares


def calc_share_rates(last_block, from_account):
    """Calculates share rates"""
    new_payment_batch = Payment_batch(last_block, from_account)
    for miner in mining.miners:
        share_rate = mining.miners[miner] / mining.shares_of_current_block
        new_payment_batch.add_payment(miner, share_rate)
        mining.miners[miner] = 0  # share to 0
    mining.shares_of_current_block = 0
    new_payment_batch.add_payment(pool_account, 0)

    new_payment_batch_text = "New payment batch: block: " + str(new_payment_batch.block) + ", from account: " + str(
        new_payment_batch.from_account) + '\n'
    for payment in new_payment_batch.payments:
        text = "To: " + str(payment) + ", " + str(new_payment_batch.payments[payment]) + '\n'
        new_payment_batch_text = new_payment_batch_text + text
    new_payment_batch_text += '\n'

    for payment in new_payment_batch.payments:
        try:
            sqlite_handler.db.add_payment_to_DB(last_block, from_account, payment, new_payment_batch.payments[payment])
        except Exception as e:
            logger.error("SQlite error at calc_share_rates: " + str(e))
            print("SQlite error")
            print(e)

    payment_batches.append(new_payment_batch)


def set_amounts(block):
    """Calculates and sets reward amounts from block reward, pool fee and share rate"""
    block_reward = wallet_json_rpc.get_block_reward(block)
    payments = sqlite_handler.db.get_payments_of_block(block)
    spent = 0
    from_account = 0
    for payment in payments:
        if payment[3] == pool_account:
            continue
        if payment[3] not in account_fees:
            account_fees[payment[3]] = pool_fee     # if there was a restart after account goes offline, there is no fee data

        from_account = payment[2]
        to_account = payment[3]
        amount = round((payment[8] * block_reward * (1 - (account_fees[payment[3]] / 100)) - payment_fee - payment_fee_to_pool), payment_prec)
        if amount > payment_fee:
            sqlite_handler.db.set_amount_for_payment(payment[1], payment[2], payment[3], amount)
            spent += amount + payment_fee
        else:
            sqlite_handler.db.remove_payment_from_DB(from_account, to_account)

    amount = round(block_reward - spent - payment_fee, payment_prec)
    if amount > payment_fee:
        sqlite_handler.db.set_amount_for_payment(block, from_account, pool_account, amount)
    else:
        sqlite_handler.db.remove_payment_from_DB(from_account, pool_account)


def payment_processor():
    """Payment processor, called by start_payment_processor"""
    global current_block
    print("\nStarting payment processor")
    block_checked = []
    block_matured = []

    result = sqlite_handler.db.get_unacked_blocks()
    for block in result:
        # block_checked neccessary to speed up. Multiple txs have the same block, enough to set once a block to checked
        if block[1] in block_checked:
            continue
        block_checked.append(block[1])

        try:
            retval = wallet_json_rpc.check_block_pubkey(block[1])
        except wallet_json_rpc.WalletCommError:
            return False

        if retval:
            sqlite_handler.db.set_block_to_acked_by_wallet(block[1])
            set_amounts(block[1])
        elif block[1] < current_block - orphan_age_limit:   # check if the block is orphan
            sqlite_handler.db.set_block_to_orphan(block[1])   # set to orphan in db
            print("Block %d marked as orphan" % block[1])

    result = sqlite_handler.db.get_unconfirmed_blocks()
    for block in result:
        if block[1] in block_matured:
            continue

        try:
            retval = wallet_json_rpc.is_block_matured(block[1])
        except wallet_json_rpc.WalletCommError:
            return False

        if retval:
            sqlite_handler.db.set_block_confirmed(block[1])
            block_matured.append(block[1])

    sqlite_handler.db.delete_zero_txs()

    result = sqlite_handler.db.get_unpaid_payments()
    try:
        wallet_json_rpc.unlock_wallet()
    except wallet_json_rpc.WalletCommError:
        return False

    for row in result:
        try:
            wallet_json_rpc.send_payment(row[2], row[3], row[4], row[1])
        except wallet_json_rpc.WalletPubKeyError:
            if row[1] < current_block - orphan_age_limit:     # block is orphan
                sqlite_handler.db.set_block_to_orphan(row[1])
        except wallet_json_rpc.WalletCommError:
            return False
        except wallet_json_rpc.WalletInvalidTargetAccountError:
            # TODO handle invalid target account. But if it's validated on auth, then no need for that.
            logger.info("Invalid target account: " + str(row[3]))
        except wallet_json_rpc.WalletInvalidOperationError:
            pass        # TODO it's probably a balance issue which occurs rarely. Sometimes payouts fails and rewards are sent twice for an account and there is no money left for the rest.
        else:
            sqlite_handler.db.set_payment_to_paid(row[1], row[2], row[3])

    return True


def start_payment_processor():
    """Starts payment_processor. Called in _main.py"""
    if not payment_processor():
        print("Payment processor error. Is wallet running?")

    threading.Timer(60, start_payment_processor).start()
