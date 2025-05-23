FROM bitnami/spark:3.5.1

USER root

# Aggiorna pip e installa librerie Python richieste
RUN pip install --upgrade pip && \
    pip install redis hdfs

# Torna a utente spark (bitnami usa l'utente '1001' o 'spark')
USER 1001

