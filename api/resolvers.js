const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

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

const resolvers = {
  Query: {
    certificat: (_, { id }) => {
      const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.getCertificat({ certificat_id: id }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    certificats: (_, { query }) => {
      const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.searchCertificats({ query }, (err, res) => {
          if (err) reject(err);
          else resolve(res.certificats);
        });
      });
    },
    competence: (_, { id }) => {
      const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.getCompetence({ competence_id: id }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    competences: (_, { query }) => {
      const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.searchCompetences({ query }, (err, res) => {
          if (err) reject(err);
          else resolve(res.competences);
        });
      });
    },
  },
  Mutation: {
    createCertificat: (_, { nom, organisation, date_obtention }) => {
      const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.createCertificat({ nom, organisation, date_obtention }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    updateCertificat: (_, { id, nom, organisation, date_obtention }) => {
      const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.updateCertificat({ 
          certificat_id: id, 
          nom, 
          organisation, 
          date_obtention 
        }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    deleteCertificat: (_, { id }) => {
      const client = new certificatProto.CertificatService('localhost:50051', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.deleteCertificat({ certificat_id: id }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    createCompetence: (_, { nom, niveau, categorie }) => {
      const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.createCompetence({ nom, niveau, categorie }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    updateCompetence: (_, { id, nom, niveau, categorie }) => {
      const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.updateCompetence({ 
          competence_id: id, 
          nom, 
          niveau, 
          categorie 
        }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    deleteCompetence: (_, { id }) => {
      const client = new competenceProto.CompetenceService('localhost:50052', grpc.credentials.createInsecure());
      return new Promise((resolve, reject) => {
        client.deleteCompetence({ competence_id: id }, (err, res) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
    }
  }
};

module.exports = resolvers;