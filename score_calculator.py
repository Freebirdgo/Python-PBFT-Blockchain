import json
import argparse
import glob
import os

def parse_blockchain_to_blocks(data_string):
    """
    Parses the multi-JSON blockchain data string into a list of block objects.
    Each block object is a dictionary.
    """
    parsed_blocks = []
    # Split the string by the separator and filter out empty strings
    json_strs = [s.strip() for s in data_string.strip().split('------------') if s.strip()]

    for js in json_strs:
        try:
            parsed_blocks.append(json.loads(js))
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON block: {e}")
            print(f"Problematic JSON string: ---{js[:200]}...---")
    return parsed_blocks

def calculate_score_for_block(block_index, block_transactions):
    """
    Calculates the score for a single block's transactions based on buy/sell matching rules.
    Rule 2: Start with the largest sell price to check if there is a larger buy price:
            Yes, add the buy price and delete the two transactions
            No, delete the sell transaction
    """
    buy_prices = []
    sell_prices = []

    print(f"\n--- Processing Block {block_index} ---")

    if not block_transactions or not isinstance(block_transactions, list):
        print("No transactions in this block or invalid format.")
        return 0

    for tx in block_transactions:
        if block_index == 0 and isinstance(tx, str): # Handle Genesis block transaction string
            print(f"Genesis block transaction: {tx}")
            continue

        # Ensure transaction is a list with 2 elements for regular blocks
        if isinstance(tx, list) and len(tx) == 2:
            data = tx[1] # Get the 'buy:xx' or 'sell:xx' part
            try:
                tx_type, price_str = data.split(':')
                price = int(price_str)
                if tx_type == 'buy':
                    buy_prices.append(price)
                elif tx_type == 'sell':
                    sell_prices.append(price)
            except ValueError:
                print(f"Skipping malformed transaction data in block {block_index}: {data}")
        elif block_index != 0 : # If not genesis and not the expected list format
             print(f"Skipping unexpected transaction format in block {block_index}: {tx}")


    # Sort both lists in descending order (highest price first)
    buy_prices.sort(reverse=True)
    sell_prices.sort(reverse=True)

    print(f"Block {block_index} - Initial Buy Prices ({len(buy_prices)}): {buy_prices}")
    print(f"Block {block_index} - Initial Sell Prices ({len(sell_prices)}): {sell_prices}")

    block_score = 0
    
    unique_sorted_sells = sorted(list(set(sell_prices)), reverse=True)

    for current_max_sell_price in unique_sorted_sells:
        while current_max_sell_price in sell_prices:
            match_buy_price = -1
            
            temp_buy_prices_sorted = sorted(buy_prices, reverse=True)
            
            for buy_price_candidate in temp_buy_prices_sorted:
                if buy_price_candidate > current_max_sell_price:
                    match_buy_price = buy_price_candidate
                    break 
            
            if match_buy_price != -1:
                print(f"Block {block_index} - Matching Sell({current_max_sell_price}) with Buy({match_buy_price})")
                block_score += match_buy_price
                sell_prices.remove(current_max_sell_price)
                buy_prices.remove(match_buy_price)
            else:
                print(f"Block {block_index} - No match for Sell({current_max_sell_price}), deleting.")
                sell_prices.remove(current_max_sell_price)

    print(f"Block {block_index} - Remaining Buy Prices: {sorted(buy_prices, reverse=True)}")
    print(f"Block {block_index} - Remaining Sell Prices: {sorted(sell_prices, reverse=True)}")
    print(f"--- Score for Block {block_index}: {block_score} ---")
    
    return block_score

def main():
    parser = argparse.ArgumentParser(description="Calculate score from a blockchain file, block by block.")
    parser.add_argument("filename", nargs='?', default=None,
                        help="Path to the blockchain file (e.g., '~$node_0.blockchain'). "
                             "If not provided, script will try to find one automatically.")
    args = parser.parse_args()

    blockchain_filepath = args.filename
    blockchain_data_string = ""

    if blockchain_filepath:
        print(f"Attempting to read specified file: {blockchain_filepath}")
        if not os.path.exists(blockchain_filepath):
            print(f"Error: File not found at {blockchain_filepath}")
            return
    else:
        print("No filename provided, attempting to find a blockchain file...")
        potential_files = glob.glob("~$node_*.blockchain")
        if not potential_files:
            potential_files = glob.glob("node_*.blockchain") # Fallback

        if potential_files:
            potential_files.sort()
            blockchain_filepath = potential_files[0]
            print(f"Found blockchain file: {blockchain_filepath}")
        else:
            print("Error: No blockchain file specified and no default blockchain files "
                  "(e.g., '~$node_0.blockchain' or 'node_0.blockchain') found in the current directory.")
            return

    try:
        with open(blockchain_filepath, 'r') as f:
            blockchain_data_string = f.read()
    except Exception as e:
        print(f"Error reading file {blockchain_filepath}: {e}")
        return

    if not blockchain_data_string.strip():
        print(f"Error: Blockchain file {blockchain_filepath} is empty or contains only whitespace.")
        return

    all_blocks = parse_blockchain_to_blocks(blockchain_data_string)
    if not all_blocks:
        print("No blocks found in the blockchain data.")
        return

    total_blockchain_score = 0
    for block in all_blocks:
        block_idx = block.get("index", "Unknown")
        block_txs = block.get("transactions", [])

        if block_idx == 0: # Genesis block
            print(f"\n--- Skipping Genesis Block (Index {block_idx}) for scoring ---")
            if isinstance(block_txs, list) and len(block_txs) > 0 and isinstance(block_txs[0], str):
                 print(f"Genesis block transaction: {block_txs[0]}")
            continue # Skip scoring for genesis block based on buy/sell rules

        block_score = calculate_score_for_block(block_idx, block_txs)
        total_blockchain_score += block_score

    print(f"\n==========================================")
    print(f"Total Score for Blockchain in {blockchain_filepath}: {total_blockchain_score}")
    print(f"==========================================")

if __name__ == "__main__":
    main()