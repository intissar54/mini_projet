const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const resolvers = require('./resolvers');
const typeDefs = require('./schema');

const certificatProtoPath = path.join(__dirname, '../protos/certificat.proto');
const competenceProtoPath = path.join(__dirname, '../protos/competence.proto');

const certificatProtoDefinition = protoLoader.loadSync(certificatProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const competenceProtoDefinition = protoLoader.loadSync(competenceProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});

const certificatProto = grpc.loadPackageDefinition(certificatProtoDefinition).certificat;
const competenceProto = grpc.loadPackageDefinition(competenceProtoDefinition).competence;

const app = express();
app.use(cors());
app.use(bodyParser.json());

// REST Endpoints pour Certificats
app.get('/certificats', (req, res) => {
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.searchCertificats({ query: req.query.q }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response.certificats);
  });
});

app.get('/certificats/:id', (req, res) => {
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.getCertificat({ certificat_id: req.params.id }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.post('/certificats', (req, res) => {
  const { nom, organisation, date_obtention } = req.body;
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.createCertificat({ nom, organisation, date_obtention }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.put('/certificats/:id', (req, res) => {
  const { nom, organisation, date_obtention } = req.body;
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.updateCertificat({ 
    certificat_id: req.params.id,
    nom,
    organisation,
    date_obtention
  }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.delete('/certificats/:id', (req, res) => {
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.deleteCertificat({ certificat_id: req.params.id }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

// REST Endpoints pour Compétences
app.get('/competences', (req, res) => {
  const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
  client.searchCompetences({ query: req.query.q }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response.competences);
  });
});

app.get('/competences/:id', (req, res) => {
  const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
  client.getCompetence({ competence_id: req.params.id }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.post('/competences', (req, res) => {
  const { nom, niveau, categorie } = req.body;
  const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
  client.createCompetence({ nom, niveau, categorie }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.put('/competences/:id', (req, res) => {
  const { nom, niveau, categorie } = req.body;
  const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
  client.updateCompetence({ 
    competence_id: req.params.id,
    nom,
    niveau,
    categorie
  }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

app.delete('/competences/:id', (req, res) => {
  const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
  client.deleteCompetence({ competence_id: req.params.id }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

// GraphQL
const server = new ApolloServer({ typeDefs, resolvers });

server.start().then(() => {
  app.use('/graphql', expressMiddleware(server));
});

const port = process.env.API_GATEWAY_PORT || 3005;
app.listen(port, () => console.log(`API Gateway sur le port ${port}`));