const { Kafka } = require('kafkajs');
const nodemailer = require('nodemailer');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

// Configuration du transporteur email
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASSWORD
  }
});

async function runConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'certification-notifications', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { type, data } = JSON.parse(message.value.toString());
      
      if (type === 'NEW_CERTIFICATION') {
        const mailOptions = {
          from: process.env.EMAIL_USER,
          to: 'entissarbenaatia@gmail.com',
          subject: `Nouvelle certification créée: ${data.nom}`,
          html: `
            <h1>Nouvelle certification enregistrée</h1>
            <p><strong>Nom:</strong> ${data.nom}</p>
            <p><strong>Organisme:</strong> ${data.organisme_delivrant}</p>
            <p><strong>Date d'obtention:</strong> ${new Date(data.date_obtention).toLocaleDateString()}</p>
            ${data.date_expiration ? `<p><strong>Date d'expiration:</strong> ${new Date(data.date_expiration).toLocaleDateString()}</p>` : ''}
            <p><strong>Compétences:</strong> ${data.competences}</p>
          `
        };

        transporter.sendMail(mailOptions, (error, info) => {
          if (error) {
            console.error('Erreur lors de l\'envoi de l\'email:', error);
          } else {
            console.log('Email de notification envoyé:', info.response);
          }
        });
      }
    }
  });
}

runConsumer().catch(console.error);