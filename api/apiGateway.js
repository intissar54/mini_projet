// api/apiGateway.js

const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { sendCertificationEvent } = require('../kafka/producer');
const resolvers = require('./resolvers');
const typeDefs = require('./schema');

// Chargement des fichiers .proto
const certificatProtoPath = path.join(__dirname, '../protos/certificat.proto');
const competenceProtoPath = path.join(__dirname, '../protos/competence.proto');

const certificatProtoDefinition = protoLoader.loadSync(certificatProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});
const competenceProtoDefinition = protoLoader.loadSync(competenceProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});

const certificatProto = grpc.loadPackageDefinition(certificatProtoDefinition).certificat;
const competenceProto = grpc.loadPackageDefinition(competenceProtoDefinition).competence;

// Express App
const app = express();
app.use(cors());
app.use(bodyParser.json());

// Logging Middleware
app.use((req, _, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// === REST Endpoints Certificats ===
app.get('/certificats', (req, res) => {
  const client = new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure());
  client.searchCertificats({ query: req.query.q || '' }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response.certificats);
  });
});

app.get('/certificats/:id', (req, res) => {
  const client = new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure());
  client.getCertificat({ certificat_id: req.params.id }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

app.post('/certificats', async (req, res) => {
  const { nom, organisation, date_obtention } = req.body;

  if (!nom || !organisation || !date_obtention) {
    return res.status(400).json({ error: 'Tous les champs sont requis' });
  }

  const client = new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure());

  client.createCertificat({ nom, organisation, date_obtention }, async (err, response) => {
    if (err) {
      console.error('Erreur création certificat:', err);
      return res.status(500).json({ error: 'Erreur création certificat' });
    }

    try {
      await sendCertificationEvent({
        action: 'CERTIFICAT_CREATED',
        certificat: {
          id: response.id,
          nom,
          organisation,
          date_obtention
        },
        timestamp: new Date().toISOString()
      });
    } catch (kafkaError) {
      console.error('Erreur Kafka:', kafkaError);
    }

    res.status(201).json(response);
  });
});

app.put('/certificats/:id', (req, res) => {
  const { nom, organisation, date_obtention } = req.body;
  const client = new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure());

  client.updateCertificat({
    certificat_id: req.params.id,
    nom,
    organisation,
    date_obtention
  }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

app.delete('/certificats/:id', (req, res) => {
  const client = new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure());
  client.deleteCertificat({ certificat_id: req.params.id }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

// === REST Endpoints Compétences ===
app.get('/competences', (req, res) => {
  const client = new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure());
  client.searchCompetences({ query: req.query.q || '' }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response.competences);
  });
});

app.get('/competences/:id', (req, res) => {
  const client = new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure());
  client.getCompetence({ competence_id: req.params.id }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

app.post('/competences', (req, res) => {
  const { nom, niveau, categorie } = req.body;
  const client = new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure());

  client.createCompetence({ nom, niveau, categorie }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

app.put('/competences/:id', (req, res) => {
  const { nom, niveau, categorie } = req.body;
  const client = new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure());

  client.updateCompetence({
    competence_id: req.params.id,
    nom,
    niveau,
    categorie
  }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

app.delete('/competences/:id', (req, res) => {
  const client = new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure());
  client.deleteCompetence({ competence_id: req.params.id }, (err, response) => {
    if (err) return res.status(500).send(err);
    res.json(response);
  });
});

// === GraphQL ===
const server = new ApolloServer({
  typeDefs,
  resolvers,
  formatError: (err) => {
    console.error('Erreur GraphQL:', err);
    return err;
  }
});

async function startServer() {
  await server.start();

  app.use('/graphql', expressMiddleware(server, {
    context: async ({ req }) => ({
      req,
      certificatClient: new certificatProto.CertificatService('certificat-service:50051', grpc.credentials.createInsecure()),
      competenceClient: new competenceProto.CompetenceService('competence-service:50052', grpc.credentials.createInsecure()),
      sendCertificationEvent
    })
  }));

  const PORT = process.env.API_GATEWAY_PORT || 3005;
  app.listen(PORT, () => {
    console.log(`🚀 API Gateway démarré sur http://localhost:${PORT}`);
    console.log(`🔮 GraphQL disponible sur http://localhost:${PORT}/graphql`);
  });
}

startServer().catch(err => {
  console.error('Erreur démarrage serveur:', err);
  process.exit(1);
});
