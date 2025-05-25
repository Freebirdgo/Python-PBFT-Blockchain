import blockchain_client as client
import logging # Import logging

def main():
    args = client.arg_parse()
    # Configure logging here for the client_app process
    client.logging_config(log_level_str=args.log_level, log_file=f'~$client_{args.client_id}.log')
    
    client_instance = client.setup(args)
    try:
        client.run_app(client_instance)
    except Exception as e:
        # Use a logger if client_instance and its logger are available
        logger = getattr(client_instance, '_log', logging.getLogger())
        logger.critical("Client app failed: %s", e, exc_info=True)

if __name__ == "__main__":
    main()