const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();

const PROTO_PATH = '../protos/certificat.proto';
<<<<<<< HEAD
const MONGO_URI = 'mongodb://localhost:27017';
=======
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
<<<<<<< HEAD
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);
  console.log('✅ Connecté à MongoDB (certificats)');
=======
  const client = new MongoClient(MONGO_URI, { useUnifiedTopology: true });
  try {
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Connecté à MongoDB (certificats)');
  } catch (error) {
    console.error('❌ Échec de la connexion à MongoDB :', error);
    process.exit(1);
  }
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
<<<<<<< HEAD
        details: 'Erreur interne lors de la récupération',
=======
        details: 'Erreur interne lors de la récupération du certificat',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
<<<<<<< HEAD
        details: 'Erreur lors de la recherche',
=======
        details: 'Erreur lors de la recherche de certificats',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
<<<<<<< HEAD
        details: 'Erreur lors de la création',
=======
        details: 'Erreur lors de la création du certificat',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
<<<<<<< HEAD
        details: 'Erreur lors de la mise à jour',
=======
        details: 'Erreur lors de la mise à jour du certificat',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
      });
    }
  },

  deleteCertificat: async (call, callback) => {
    const { certificat_id } = call.request;

    if (!ObjectId.isValid(certificat_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
<<<<<<< HEAD
        details: 'ID invalide',
=======
        details: 'ID de certificat invalide',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
      });
    }

    try {
      const result = await db.collection('certificats').deleteOne({
        _id: new ObjectId(certificat_id),
      });

      if (result.deletedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
<<<<<<< HEAD
          details: 'Certificat non trouvé',
=======
          details: 'Certificat non trouvé pour la suppression',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
        });
      }

      callback(null, { success: true });
    } catch (error) {
      callback({
        code: grpc.status.INTERNAL,
<<<<<<< HEAD
        details: 'Erreur lors de la suppression',
=======
        details: 'Erreur lors de la suppression du certificat',
>>>>>>> b955a6a13e6936548dc1d855e8783ac22b452c91
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
