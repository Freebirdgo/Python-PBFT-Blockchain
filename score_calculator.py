import json
import argparse
import glob
import os

def parse_blockchain_data(data_string):
    """
    Parses the multi-JSON blockchain data string and extracts transactions.
    """
    blocks = []
    # Split the string by the separator and filter out empty strings
    json_strs = [s.strip() for s in data_string.strip().split('------------') if s.strip()]

    for js in json_strs:
        try:
            blocks.append(json.loads(js))
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON block: {e}")
            print(f"Problematic JSON string: ---{js[:200]}...---")

    transactions = []
    for block in blocks:
        # Skip the genesis block's transaction
        if block.get("index") == 0:
            continue
        # Ensure 'transactions' exists and is a list
        if "transactions" in block and isinstance(block["transactions"], list):
            transactions.extend(block["transactions"])
    return transactions

def calculate_score(transactions):
    """
    Calculates the score based on buy/sell matching rules.
    Rule 2: Start with the largest sell price to check if there is a larger buy price:
            Yes, add the buy price and delete the two transactions
            No, delete the sell transaction
    """
    buy_prices = []
    sell_prices = []

    for tx in transactions:
        # Ensure transaction is a list with 2 elements
        if isinstance(tx, list) and len(tx) == 2:
            # tx[0] is the client_id string like "[6, 0]"
            data = tx[1] # Get the 'buy:xx' or 'sell:xx' part
            try:
                tx_type, price_str = data.split(':')
                price = int(price_str)
                if tx_type == 'buy':
                    buy_prices.append(price)
                elif tx_type == 'sell':
                    sell_prices.append(price)
            except ValueError:
                print(f"Skipping malformed transaction data: {data}")

    # Sort both lists in descending order (highest price first)
    buy_prices.sort(reverse=True)
    sell_prices.sort(reverse=True)

    print(f"Initial Buy Prices ({len(buy_prices)}): {buy_prices}")
    print(f"Initial Sell Prices ({len(sell_prices)}): {sell_prices}")

    score = 0
    
    # Iterate through sell prices (highest first). We need to be careful when modifying lists we iterate over.
    # A common pattern is to iterate over a copy or use indices with a while loop if elements are removed.
    # For this rule, we process sell_prices one by one from highest.
    
    processed_sell_prices = [] # To keep track of sells that have been considered

    # Iterating over a sorted list of unique sell prices (descending)
    # This handles cases where sell_prices might have duplicates and ensures each unique sell value is processed once as the "largest current sell"
    unique_sorted_sells = sorted(list(set(sell_prices)), reverse=True)

    for current_max_sell_price in unique_sorted_sells:
        # Process all instances of this current_max_sell_price
        while current_max_sell_price in sell_prices:
            # Find the highest buy price strictly greater than the current_max_sell_price
            match_buy_price = -1
            match_buy_index = -1

            # Iterate through a copy of buy_prices sorted descending to find the best match
            # (Highest buy price > current_max_sell_price)
            temp_buy_prices_sorted = sorted(buy_prices, reverse=True)
            
            for i, buy_price_candidate in enumerate(temp_buy_prices_sorted):
                if buy_price_candidate > current_max_sell_price:
                    # To select the actual element from the original buy_prices list
                    # we need to find its original index if we were to pop by value.
                    # It's safer to find the highest and then remove that specific value.
                    # Here, since temp_buy_prices_sorted is sorted, the first one we find is the highest.
                    match_buy_price = buy_price_candidate
                    break 
            
            if match_buy_price != -1:
                # Match found!
                print(f"Matching Sell({current_max_sell_price}) with Buy({match_buy_price})")
                score += match_buy_price
                sell_prices.remove(current_max_sell_price) # Remove one instance of this sell price
                buy_prices.remove(match_buy_price)       # Remove the matched buy price
            else:
                # No match found, remove this instance of the sell transaction
                print(f"No match for Sell({current_max_sell_price}), deleting.")
                sell_prices.remove(current_max_sell_price) # Remove one instance of this sell price

    print(f"\nRemaining Buy Prices after matching: {sorted(buy_prices, reverse=True)}")
    print(f"Remaining Sell Prices after matching: {sorted(sell_prices, reverse=True)}")
    
    return score

def main():
    parser = argparse.ArgumentParser(description="Calculate score from a blockchain file.")
    parser.add_argument("filename", nargs='?', default=None,
                        help="Path to the blockchain file (e.g., ~\\$node_0.blockchain). "
                             "If not provided, script will try to find one automatically.")
    args = parser.parse_args()

    blockchain_filepath = args.filename
    blockchain_data = ""

    if blockchain_filepath:
        print(f"Attempting to read specified file: {blockchain_filepath}")
        if not os.path.exists(blockchain_filepath):
            print(f"Error: File not found at {blockchain_filepath}")
            return
    else:
        print("No filename provided, attempting to find a blockchain file...")
        # Pattern from node.py: ~$node_{self._index}.blockchain
        # The "~$" might be an issue with glob if it's a shell expansion.
        # Let's try with the literal characters first, then without if it fails.
        # Usually, these files are in the same directory as the node.py.
        
        # First, look for files starting with "~$node_"
        potential_files = glob.glob("~$node_*.blockchain") #
        
        if not potential_files:
            # If not found, look for files just starting with "node_"
            # This is in case the "~$" prefix was problematic or not always used
            potential_files = glob.glob("node_*.blockchain")

        if potential_files:
            # Sort to get a consistent choice, e.g., node_0 if multiple exist
            potential_files.sort()
            blockchain_filepath = potential_files[0]
            print(f"Found blockchain file: {blockchain_filepath}")
        else:
            print("Error: No blockchain file specified and no default blockchain files "
                  "(e.g., '~$node_0.blockchain' or 'node_0.blockchain') found in the current directory.")
            return

    try:
        with open(blockchain_filepath, 'r') as f:
            blockchain_data = f.read()
    except Exception as e:
        print(f"Error reading file {blockchain_filepath}: {e}")
        return

    if not blockchain_data.strip():
        print(f"Error: Blockchain file {blockchain_filepath} is empty or contains only whitespace.")
        return

    all_transactions = parse_blockchain_data(blockchain_data)
    if not all_transactions:
        print("No transactions found in the blockchain (excluding genesis).")
        # If you still want to "calculate" a score of 0:
        # final_score = 0
        # print(f"\nFinal Score: {final_score}")
        return


    final_score = calculate_score(all_transactions)
    print(f"\nFinal Score for {blockchain_filepath}: {final_score}")

if __name__ == "__main__":
    main()