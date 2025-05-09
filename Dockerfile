FROM node:16

WORKDIR /app

# Copier les fichiers de dépendances (avant pour profiter du cache Docker)
COPY package*.json ./

RUN npm install

# Copier tout le reste des fichiers
COPY . .

EXPOSE 3000 50051 50052

# Commande par défaut (cela dépend du service que vous souhaitez démarrer)
CMD ["node", "api/apiGateway.js"]
