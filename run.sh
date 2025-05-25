./run_node.sh
sleep 10 
./run_client.sh

sleep 100
pkill -9 -f node.py
pkill -9 -f client_app.py