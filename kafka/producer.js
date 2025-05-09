const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'certification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();

async function sendCertificationNotification(certificationData) {
  try {
    await producer.connect();
    await producer.send({
      topic: 'certification-notifications',
      messages: [
        { 
          value: JSON.stringify({
            type: 'NEW_CERTIFICATION',
            data: certificationData
          }) 
        }
      ]
    });
  } catch (err) {
    console.error('Erreur Kafka producer:', err);
  } finally {
    await producer.disconnect();
  }
}

module.exports = { sendCertificationNotification };