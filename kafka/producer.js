const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();

async function sendCertificationEvent(event) {
  try {
    await producer.connect();
    await producer.send({
      topic: 'certification-events',
      messages: [
        { value: JSON.stringify(event) }
      ]
    });
    console.log('✅ Événement Kafka envoyé:', event.action);
  } catch (error) {
    console.error('❌ Erreur envoi Kafka:', error);
  } finally {
    await producer.disconnect();
  }
}

module.exports = { sendCertificationEvent };