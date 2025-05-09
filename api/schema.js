const { gql } = require('@apollo/server');

const typeDefs = `#graphql
  type Certificat {
    id: String!
    nom: String!
    organisation: String!
    date_obtention: String!
  }

  type Competence {
    id: String!
    nom: String!
    niveau: String!
    categorie: String!
  }

  type DeleteResponse {
    success: Boolean!
  }

  type Query {
    certificat(id: String!): Certificat
    certificats(query: String): [Certificat]
    competence(id: String!): Competence
    competences(query: String): [Competence]
  }

  type Mutation {
    createCertificat(nom: String!, organisation: String!, date_obtention: String!): Certificat
    updateCertificat(id: String!, nom: String, organisation: String, date_obtention: String): Certificat
    deleteCertificat(id: String!): DeleteResponse
    createCompetence(nom: String!, niveau: String!, categorie: String!): Competence
    updateCompetence(id: String!, nom: String, niveau: String, categorie: String): Competence
    deleteCompetence(id: String!): DeleteResponse
  }
`;

module.exports = typeDefs;