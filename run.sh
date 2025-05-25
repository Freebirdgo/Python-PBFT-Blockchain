./run_node.sh
sleep 15 
./run_client.sh

sleep 50
pkill -9 -f node.py
pkill -9 -f client_app.py