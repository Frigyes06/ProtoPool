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
    def __init__(self, block, from_account):
        self.block = block
        self.from_account = from_account
        self.payments = {}
        self.paid = False

    def add_payment(self, account, share_rate):
        self.payments[account] = share_rate

def new_block_accountancy():
    global current_block
    #TODO try-except
#    last_block = wallet_json_rpc.get_last_block()

#    last_account = wallet_json_rpc.get_last_account()

#    print("Last account:" + str(last_account["account"]))
#    print("Last reward: " + str(last_account["balance"]))
#    if last_account["account"] == 0:
#        print("Last account can't be found!!!!!!!!!!!! ERROR")

    if not sqlite_handler.db.is_block_in_db_already(current_block):
        calc_shares()
        calc_share_rates(current_block, current_block*5)
#    calc_payments(current_block, last_account["balance"], last_account["account"])

def calc_shares():
    current_time = time.time()
    mining.shares_of_current_block = 0
    for account in mining.shares:
        account_shares = 0
        for timestamp in list(mining.shares[account].timestamps):
            if timestamp >= current_time - pplns_interval:
                #print("Share counted: " + str(timestamp))
                account_shares += mining.shares[account].timestamps[timestamp]
            else:
                #print("Share with deleted: " + str(timestamp))
                del mining.shares[account].timestamps[timestamp]
        mining.miners[account] = account_shares
        mining.shares_of_current_block += account_shares
        #print("Current block shares: " + str(mining.shares_of_current_block))

def calc_share_rates(last_block, from_account):
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
        #print(text)
    new_payment_batch_text += '\n'

    # Write to DB
    for payment in new_payment_batch.payments:
        try:
            sqlite_handler.db.add_payment_to_DB(last_block, from_account, payment, new_payment_batch.payments[payment])
        except Exception as e:
            logger.error("SQlite error at calc_share_rates: " + str(e))
            print("SQlite error")
            print(e)

    payment_batches.append(new_payment_batch)

def set_amounts(block):
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

#not used
def calc_payments(last_block, last_reward, from_account):
    global account_fees
    spent = 0
    new_payment_batch = Payment_batch(last_block, from_account)
    for miner in mining.miners:
        amount = round((mining.miners[miner] / mining.shares_of_current_block * last_reward * (1 - (account_fees[miner]/100))-payment_fee), payment_prec)
        if amount <= 0:
            continue
        new_payment_batch.add_payment(miner, amount)
        spent += amount + payment_fee
        mining.miners[miner] = 0        # share to 0
    mining.shares_of_current_block = 0
    new_payment_batch.add_payment(pool_account, round(last_reward-spent-payment_fee, payment_prec))

    new_payment_batch_text = "New payment batch: block: " + str(new_payment_batch.block) + ", from account: " + str(new_payment_batch.from_account) + ", sum reward: " + str(new_payment_batch.sum_reward) + '\n'
    for payment in new_payment_batch.payments:
        text = "To: " + str(payment) + ", " + str(new_payment_batch.payments[payment]) + '\n'
        new_payment_batch_text = new_payment_batch_text + text
        print(text)
    new_payment_batch_text += '\n'
    filename = "payments.txt"
    f = open(filename, "a")
    f.write(new_payment_batch_text)
    f.close()

    # Write to DB
    for payment in new_payment_batch.payments:
        try:
            sqlite_handler.db.add_payment_to_DB(last_block, from_account, payment, new_payment_batch.payments[payment])
        except Exception as e:
            logger.error("SQlite error at calc_payments: " + str(e))
            print("SQlite error")

    payment_batches.append(new_payment_batch)

#not used
def do_payment_batch():
    global payment_batches

    nothing_to_pay = True
    for payment_batch in payment_batches:
        if payment_batch.paid is False:
            # DO payments
            #create multioperation
            payment_batch_can_be_paid = True
            try:
                wallet_json_rpc.unlock_wallet()
            except:
                break
            for account in payment_batch.payments:
                #TODO check in DB if it's paid or not
                result = wallet_json_rpc.send_payment(payment_batch.from_account, account, payment_batch.payments[account], payment_batch.block)

                if result is False:
                    payment_batch_can_be_paid = False
                    break
                if result is not True:
                    if "code" in result:
                        if result["code"] == 1004:
                            payment_batch_can_be_paid = False
                            break
                else:
                    try:
                        sqlite_handler.db.set_payment_to_paid(payment_batch.block, payment_batch.from_account, account)
                    except:
                        print("SQlite error")
            #TODO write to file
            if payment_batch_can_be_paid is True:
                nothing_to_pay = False
                payment_batch.paid = True  # TODO igy szemeteli a memoriat, fixalni, torolni
                print("Successful payments!")

    if nothing_to_pay:
        print("Nothing to pay")
    threading.Timer(60,do_payment_batch).start()

def payment_processor():
    global current_block
    #print("\nStarting payment processor")
    block_checked = []
    block_matured = []

    result = sqlite_handler.db.get_unacked_blocks()
    for block in result:
        #block_checked neccessary to speed up. Multiple txs have the same block, enough to set once a block to checked
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
    if not payment_processor():
        print("Payment processor error. Is wallet running?")

    threading.Timer(60, start_payment_processor).start()
