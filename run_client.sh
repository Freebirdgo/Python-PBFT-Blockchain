#!/bin/sh

for i in  {0..19} 
do
   python3 ./client_app.py -id $i -nm 10 & 
done
