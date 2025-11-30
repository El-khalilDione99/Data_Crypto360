# FROM python:3.10-slim

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# CMD ["python", "main.py"]

FROM python:3.10-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier TOUT le contenu de Collect_Data dans /app
COPY . /app

# Le main.py sera dans /app/main.py
# Les modules seront dans /app/APIS_binance/, etc.

# Commande par défaut
CMD ["python", "main.py"]