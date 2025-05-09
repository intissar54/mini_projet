const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();

const PROTO_PATH = '../protos/certificat.proto';
const MONGO_URI = 'mongodb://localhost:27017';
const DB_NAME = 'monmicroservice';

let db;

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const certificatProto = grpc.loadPackageDefinition(packageDefinition).certificat;

async function connectToDatabase() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);
  console.log('✅ Connecté à MongoDB (certificats)');
}

const certificatService = {
  getCertificat: async (call, callback) => {
    const { certificat_id } = call.request;

    if (!ObjectId.isValid(certificat_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID de certificat invalide',
      });
    }

    try {
      const certificat = await db.collection('certificats').findOne({
        _id: new ObjectId(certificat_id),
      });

      if (!certificat) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Certificat non trouvé',
        });
      }

      callback(null, {
        id: certificat._id.toString(),
        nom: certificat.nom,
        organisation: certificat.organisation,
        date_obtention: certificat.date_obtention,
      });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur interne lors de la récupération',
      });
    }
  },

  searchCertificats: async (call, callback) => {
    const query = call.request.query?.trim() || '';

    try {
      const results = await db.collection('certificats')
        .find({
          $or: [
            { nom: { $regex: query, $options: 'i' } },
            { organisation: { $regex: query, $options: 'i' } },
          ],
        })
        .toArray();

      const response = results.map(cert => ({
        id: cert._id.toString(),
        nom: cert.nom,
        organisation: cert.organisation,
        date_obtention: cert.date_obtention,
      }));

      callback(null, { certificats: response });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la recherche',
      });
    }
  },

  createCertificat: async (call, callback) => {
    const { nom, organisation, date_obtention } = call.request;

    if (!nom || !organisation || !date_obtention) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'Tous les champs (nom, organisation, date_obtention) sont requis',
      });
    }

    try {
      const result = await db.collection('certificats').insertOne({
        nom,
        organisation,
        date_obtention,
        createdAt: new Date(),
        updatedAt: new Date(),
      });

      callback(null, {
        id: result.insertedId.toString(),
        nom,
        organisation,
        date_obtention,
      });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la création',
      });
    }
  },

  updateCertificat: async (call, callback) => {
    const { certificat_id, nom, organisation, date_obtention } = call.request;

    if (!ObjectId.isValid(certificat_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID de certificat invalide',
      });
    }

    const updateData = {
      updatedAt: new Date(),
    };
    if (nom) updateData.nom = nom;
    if (organisation) updateData.organisation = organisation;
    if (date_obtention) updateData.date_obtention = date_obtention;

    try {
      const result = await db.collection('certificats').updateOne(
        { _id: new ObjectId(certificat_id) },
        { $set: updateData }
      );

      if (result.matchedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Certificat non trouvé pour la mise à jour',
        });
      }

      const updated = await db.collection('certificats').findOne({
        _id: new ObjectId(certificat_id),
      });

      callback(null, {
        id: updated._id.toString(),
        nom: updated.nom,
        organisation: updated.organisation,
        date_obtention: updated.date_obtention,
      });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la mise à jour',
      });
    }
  },

  deleteCertificat: async (call, callback) => {
    const { certificat_id } = call.request;

    if (!ObjectId.isValid(certificat_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID invalide',
      });
    }

    try {
      const result = await db.collection('certificats').deleteOne({
        _id: new ObjectId(certificat_id),
      });

      if (result.deletedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Certificat non trouvé',
        });
      }

      callback(null, { success: true });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la suppression',
      });
    }
  },
};

connectToDatabase().then(() => {
  const server = new grpc.Server();
  server.addService(certificatProto.CertificatService.service, certificatService);

  const PORT = process.env.GRPC_CERTIFICAT_PORT || 50051;
  server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('❌ Échec du démarrage du serveur :', err);
      process.exit(1);
    }

    console.log(`🚀 Microservice Certificats en écoute sur le port ${port}`);
    server.start();
  });
}).catch((err) => {
  console.error('❌ Connexion MongoDB échouée :', err);
  process.exit(1);
});