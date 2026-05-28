const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-admin', brokers: [BROKER] });
const admin = kafka.admin();

async function setup() {
  await admin.connect();

  await admin.createTopics({
    topics: [
      { topic: 'order-events', numPartitions: 3, replicationFactor: 1 },
      { topic: 'delivery-guarantee-demo', numPartitions: 1, replicationFactor: 1 }
    ]
  });
  console.log('✅ Topic created: order-events (3 partitions)');
  console.log('✅ Topic created: delivery-guarantee-demo (1 partition)');

  await admin.disconnect();
}

setup().catch(err => {
  console.error('❌ Failed to create topics:', err.message);
  process.exit(1);
});
