#!/bin/bash

# Formatta il NameNode solo se non è già inizializzato
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
    
    # Avvia temporaneamente il NameNode in background per poter creare le cartelle
    hdfs --daemon start namenode
    sleep 10 #5  # Attendi un po' per avvio
    
    echo "Creating HDFS directory /output..."
    hdfs dfs -mkdir -p /output
    hdfs dfs -chmod -R 777 /output

    # Termina il NameNode temporaneo
    hdfs --daemon stop namenode
fi

# Avvia normalmente il NameNode in foreground
hdfs namenode
