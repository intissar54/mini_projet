require('dotenv').config();
const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { MongoClient, ObjectId } = require('mongodb');
const { sendCertificationNotification } = require('../kafka/producer');
const { runConsumer } = require('../kafka/consumer');
const resolvers = require('./resolvers');
const typeDefs = require('./schema');

// Chargement des protos avec le bon chemin
const certificatProtoPath = path.join(__dirname, '../protos/certificat.proto');
const competenceProtoPath = path.join(__dirname, '../protos/competence.proto');

const certificatProtoDefinition = protoLoader.loadSync(certificatProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const competenceProtoDefinition = protoLoader.loadSync(competenceProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const certificatProto = grpc.loadPackageDefinition(certificatProtoDefinition).certificat;
const competenceProto = grpc.loadPackageDefinition(competenceProtoDefinition).competence;

let db;

async function connectDB() {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  db = client.db();
  console.log('Connecté à MongoDB pour Certificats');
}

const certificatService = {
  getCertificat: async (call, callback) => {
    try {
      if (!ObjectId.isValid(call.request.certificat_id)) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          details: 'Format ID invalide'
        });
      }

      const certificat = await db.collection('certificats').findOne({ 
        _id: new ObjectId(call.request.certificat_id) 
      });

      if (!certificat) {
        return callback({ 
          code: grpc.status.NOT_FOUND, 
          details: 'Certificat non trouvé' 
        });
      }

      callback(null, {
        id: certificat._id.toString(),
        nom: certificat.nom,
        organisme_delivrant: certificat.organisme_delivrant,
        date_obtention: certificat.date_obtention.toISOString(),
        date_expiration: certificat.date_expiration?.toISOString(),
        competences: certificat.competences
      });
    } catch (err) {
      console.error(err);
      callback({ 
        code: grpc.status.INTERNAL, 
        details: 'Erreur serveur' 
      });
    }
  },

  searchCertificats: async (call, callback) => {
    try {
      const query = call.request.query || '';
      const certificats = await db.collection('certificats')
        .find({ 
          $or: [
            { nom: { $regex: query, $options: 'i' } },
            { organisme_delivrant: { $regex: query, $options: 'i' } },
            { competences: { $regex: query, $options: 'i' } }
          ]
        })
        .toArray();

      callback(null, {
        certificats: certificats.map(cert => ({
          id: cert._id.toString(),
          nom: cert.nom,
          organisme_delivrant: cert.organisme_delivrant,
          date_obtention: cert.date_obtention.toISOString(),
          date_expiration: cert.date_expiration?.toISOString(),
          competences: cert.competences
        }))
      });
    } catch (err) {
      console.error(err);
      callback({ 
        code: grpc.status.INTERNAL, 
        details: 'Erreur serveur' 
      });
    }
  },

  createCertificat: async (call, callback) => {
    try {
      const { nom, organisme_delivrant, date_obtention, date_expiration, competences } = call.request;
      
      if (!nom || !organisme_delivrant || !date_obtention) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          details: 'Nom, organisme et date d\'obtention requis'
        });
      }

      const result = await db.collection('certificats').insertOne({
        nom,
        organisme_delivrant,
        date_obtention: new Date(date_obtention),
        date_expiration: date_expiration ? new Date(date_expiration) : null,
        competences,
        createdAt: new Date(),
        updatedAt: new Date()
      });

      const newCertificat = {
        id: result.insertedId.toString(),
        nom,
        organisme_delivrant,
        date_obtention,
        date_expiration,
        competences
      };

      // Envoyer la notification Kafka
      await sendCertificationNotification(newCertificat);

      callback(null, newCertificat);
    } catch (err) {
      console.error(err);
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur serveur'
      });
    }
  },

  updateCertificat: async (call, callback) => {
    try {
      const { certificat_id, nom, organisme_delivrant, date_obtention, date_expiration, competences } = call.request;

      if (!ObjectId.isValid(certificat_id)) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          details: 'Format ID invalide'
        });
      }

      const updateData = { updatedAt: new Date() };
      if (nom) updateData.nom = nom;
      if (organisme_delivrant) updateData.organisme_delivrant = organisme_delivrant;
      if (date_obtention) updateData.date_obtention = new Date(date_obtention);
      if (date_expiration) updateData.date_expiration = new Date(date_expiration);
      if (competences) updateData.competences = competences;

      const result = await db.collection('certificats').updateOne(
        { _id: new ObjectId(certificat_id) },
        { $set: updateData }
      );

      if (result.matchedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Certificat non trouvé'
        });
      }

      const updatedCertificat = await db.collection('certificats').findOne({
        _id: new ObjectId(certificat_id)
      });

      callback(null, {
        id: updatedCertificat._id.toString(),
        nom: updatedCertificat.nom,
        organisme_delivrant: updatedCertificat.organisme_delivrant,
        date_obtention: updatedCertificat.date_obtention.toISOString(),
        date_expiration: updatedCertificat.date_expiration?.toISOString(),
        competences: updatedCertificat.competences
      });
    } catch (err) {
      console.error(err);
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur serveur'
      });
    }
  },

  deleteCertificat: async (call, callback) => {
    try {
      const { certificat_id } = call.request;

      if (!ObjectId.isValid(certificat_id)) {
        return callback({
          code: grpc.status.INVALID_ARGUMENT,
          details: 'Format ID invalide'
        });
      }

      const result = await db.collection('certificats').deleteOne({
        _id: new ObjectId(certificat_id)
      });

      if (result.deletedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Certificat non trouvé'
        });
      }

      callback(null, { success: true });
    } catch (err) {
      console.error(err);
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur serveur'
      });
    }
  }
};

connectDB().then(() => {
  const grpcServer = new grpc.Server();
  grpcServer.addService(certificatProto.CertificatService.service, certificatService);
  const port = process.env.GRPC_CERTIFICAT_PORT || 50051;

  grpcServer.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('Échec du démarrage du serveur Certificat:', err);
      process.exit(1);
    }
    console.log(`Microservice Certificat en écoute sur le port ${port}`);
    grpcServer.start();
  });
}).catch(err => {
  console.error('Échec de la connexion à MongoDB:', err);
  process.exit(1);
});

// API Gateway avec Express et GraphQL
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
  const { nom, organisme_delivrant, date_obtention, date_expiration, competences } = req.body;
  const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
  client.createCertificat({ 
    nom, 
    organisme_delivrant, 
    date_obtention, 
    date_expiration, 
    competences 
  }, (err, response) => {
    if (err) res.status(500).send(err);
    else res.json(response);
  });
});

// GraphQL
const server = new ApolloServer({ typeDefs, resolvers });

server.start().then(() => {
  app.use('/graphql', expressMiddleware(server));
});

const port = process.env.API_GATEWAY_PORT || 3000;
app.listen(port, () => {
  console.log(`API Gateway sur le port ${port}`);
  // Démarrer le consumer Kafka
  runConsumer().catch(err => console.error('Erreur Kafka consumer:', err));
});

