FROM node:16-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3005 50051 50052

# Pas de CMD ici, il est défini dans docker-compose.yml