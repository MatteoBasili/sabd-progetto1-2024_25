#!/bin/bash

# Inizializza solo se la directory è vuota
if [ ! -f /opt/hadoop/data/dataNode/current/VERSION ]; then
  echo "Prima inizializzazione del DataNode: formatto la directory"
  rm -rf /opt/hadoop/data/dataNode/*
  chown -R hadoop:hadoop /opt/hadoop/data/dataNode
  chmod 755 /opt/hadoop/data/dataNode
else
  echo "DataNode già inizializzato, non cancello nulla"
fi

hdfs datanode

