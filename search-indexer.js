const { Kafka } = require('kafkajs');

// In production: this will update Elasticsearch popularity scores
const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-search', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'search-indexer' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
  console.log('🔍 Search Indexer started, boosting Elasticsearch scores...\n');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const o = JSON.parse(message.value.toString());
      console.log(`🔍 [Elasticsearch] Boosting score: ${o.restaurantId}`);
    }
  });
}

start().catch(err => {
  console.error('❌ Search Indexer error:', err.message);
  process.exit(1);
});
