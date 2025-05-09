FROM node:16

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000 50051 50052

CMD ["node", "api/apiGateway.js"]