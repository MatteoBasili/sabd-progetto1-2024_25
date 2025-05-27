#!/bin/bash

# Formatta il NameNode solo se non è già inizializzato
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
fi

# Avvia normalmente il NameNode
hdfs namenode
