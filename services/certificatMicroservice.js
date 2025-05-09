const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { MongoClient, ObjectId } = require('mongodb');
const path = require('path');
require('dotenv').config();
const { sendCertificationNotification } = require('../kafka/producer');

const protoPath = path.join(__dirname, '../protos/certificat.proto');
const protoDefinition = protoLoader.loadSync(protoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const certificatProto = grpc.loadPackageDefinition(protoDefinition).certificat;

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
  const server = new grpc.Server();
  server.addService(certificatProto.CertificatService.service, certificatService);
  const port = process.env.GRPC_CERTIFICAT_PORT || 50051;
  
  server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('Échec du démarrage du serveur Certificat:', err);
      process.exit(1);
    }
    console.log(`Microservice Certificat en écoute sur le port ${port}`);
    server.start();
  });
}).catch(err => {
  console.error('Échec de la connexion à MongoDB:', err);
  process.exit(1);
});