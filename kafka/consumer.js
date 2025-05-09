const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'certification-logger',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'certification-group' });

async function startConsumer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'certification-events', fromBeginning: true });

    console.log('👂 Consumer Kafka prêt à recevoir des messages...');

    await consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value.toString());
        console.log('\n📬 Nouvel événement certification:');
        console.log(`Action: ${event.action}`);
        console.log(`Date: ${event.timestamp}`);
        console.log('Détails:', event.certificat);
        console.log('-----------------------------------');
      }
    });
  } catch (error) {
    console.error('❌ Erreur consumer Kafka:', error);
    process.exit(1);
  }
}

startConsumer().catch(console.error);