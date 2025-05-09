const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();

const PROTO_PATH = '../protos/competence.proto';
const MONGO_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = 'monmicroservice';

let db;

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const competenceProto = grpc.loadPackageDefinition(packageDefinition).competence;

async function connectDB() {
  if (!MONGO_URI.startsWith('mongodb://') && !MONGO_URI.startsWith('mongodb+srv://')) {
    throw new Error('❌ URI MongoDB invalide : ' + MONGO_URI);
  }

  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);
  console.log('✅ Connecté à MongoDB (compétences)');
}

const competenceService = {
  getCompetence: async (call, callback) => {
    const { competence_id } = call.request;

    if (!ObjectId.isValid(competence_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID de compétence invalide',
      });
    }

    try {
      const competence = await db.collection('competences').findOne({
        _id: new ObjectId(competence_id),
      });

      if (!competence) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Compétence non trouvée',
        });
      }

      callback(null, {
        id: competence._id.toString(),
        nom: competence.nom,
        niveau: competence.niveau,
        categorie: competence.categorie,
      });
    } catch (err) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la récupération',
      });
    }
  },

  searchCompetences: async (call, callback) => {
    const query = call.request.query?.trim() || '';

    try {
      const results = await db.collection('competences').find({
        $or: [
          { nom: { $regex: query, $options: 'i' } },
          { categorie: { $regex: query, $options: 'i' } },
        ],
      }).toArray();

      const response = results.map(comp => ({
        id: comp._id.toString(),
        nom: comp.nom,
        niveau: comp.niveau,
        categorie: comp.categorie,
      }));

      callback(null, { competences: response });
    } catch (err) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la recherche',
      });
    }
  },

  createCompetence: async (call, callback) => {
    const { nom, niveau, categorie } = call.request;

    if (!nom || !niveau || !categorie) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'Tous les champs (nom, niveau, categorie) sont requis',
      });
    }

    try {
      const result = await db.collection('competences').insertOne({
        nom,
        niveau,
        categorie,
        createdAt: new Date(),
        updatedAt: new Date(),
      });

      callback(null, {
        id: result.insertedId.toString(),
        nom,
        niveau,
        categorie,
      });
    } catch (err) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la création',
      });
    }
  },

  updateCompetence: async (call, callback) => {
    const { competence_id, nom, niveau, categorie } = call.request;

    if (!ObjectId.isValid(competence_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID invalide',
      });
    }

    const updateData = { updatedAt: new Date() };
    if (nom) updateData.nom = nom;
    if (niveau) updateData.niveau = niveau;
    if (categorie) updateData.categorie = categorie;

    try {
      const result = await db.collection('competences').updateOne(
        { _id: new ObjectId(competence_id) },
        { $set: updateData }
      );

      if (result.matchedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Compétence non trouvée pour mise à jour',
        });
      }

      const updated = await db.collection('competences').findOne({
        _id: new ObjectId(competence_id),
      });

      callback(null, {
        id: updated._id.toString(),
        nom: updated.nom,
        niveau: updated.niveau,
        categorie: updated.categorie,
      });
    } catch (err) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la mise à jour',
      });
    }
  },

  deleteCompetence: async (call, callback) => {
    const { competence_id } = call.request;

    if (!ObjectId.isValid(competence_id)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'ID invalide',
      });
    }

    try {
      const result = await db.collection('competences').deleteOne({
        _id: new ObjectId(competence_id),
      });

      if (result.deletedCount === 0) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: 'Compétence non trouvée',
        });
      }

      callback(null, { success: true });
    } catch (err) {
      callback({
        code: grpc.status.INTERNAL,
        details: 'Erreur lors de la suppression',
      });
    }
  },
};

connectDB().then(() => {
  const server = new grpc.Server();
  server.addService(competenceProto.CompetenceService.service, competenceService);

  const PORT = process.env.GRPC_COMPETENCE_PORT || 50052;
  server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('❌ Échec du démarrage du serveur :', err);
      process.exit(1);
    }

    console.log(`🚀 Microservice Compétences en écoute sur le port ${port}`);
    server.start();
  });
}).catch(err => {
  console.error('❌ Échec connexion MongoDB :', err.message);
  process.exit(1);
});