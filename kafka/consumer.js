const { Kafka } = require('kafkajs');
const nodemailer = require('nodemailer');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASSWORD
  }
});

async function sendNotificationEmail(certification) {
  const mailOptions = {
    from: process.env.EMAIL_USER,
    to: 'entissarbenaatia@gmail.com',
    subject: `Nouvelle certification: ${certification.nom}`,
    html: `
      <h1>Nouvelle certification créée</h1>
      <p><strong>Nom:</strong> ${certification.nom}</p>
      <p><strong>Organisme:</strong> ${certification.organisme_delivrant}</p>
      <p><strong>Date obtention:</strong> ${new Date(certification.date_obtention).toLocaleDateString()}</p>
      <p><strong>Compétences:</strong> ${certification.competences}</p>
    `
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    console.log('Email envoyé:', info.response);
  } catch (err) {
    console.error('Erreur envoi email:', err);
  }
}

async function runConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'certification-notifications', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const { type, data } = JSON.parse(message.value.toString());
      if (type === 'NEW_CERTIFICATION') {
        await sendNotificationEmail(data);
      }
    }
  });
}

module.exports = { runConsumer };