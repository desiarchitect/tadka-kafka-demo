const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({
  clientId: 'tadka-admin',
  brokers: [BROKER],
  logLevel: 1, // WARN - suppress INFO/DEBUG noise
});
const admin = kafka.admin();

const TOPICS = [
  { topic: 'order-events', numPartitions: 3, replicationFactor: 1 },
  { topic: 'delivery-at-most-once-demo', numPartitions: 1, replicationFactor: 1 },
  { topic: 'delivery-at-least-once-demo', numPartitions: 1, replicationFactor: 1 },
  { topic: 'idempotent-consumer-demo', numPartitions: 1, replicationFactor: 1 },
];

async function setup() {
  await admin.connect();

  const existing = new Set(await admin.listTopics());

  const toCreate = TOPICS.filter(t => !existing.has(t.topic));
  const alreadyExist = TOPICS.filter(t => existing.has(t.topic));

  alreadyExist.forEach(t =>
    console.log(`ℹ️  Topic already exists: ${t.topic}`)
  );

  if (toCreate.length > 0) {
    await admin.createTopics({ topics: toCreate });
    toCreate.forEach(t =>
      console.log(`✅ Topic created: ${t.topic} (${t.numPartitions} partition${t.numPartitions > 1 ? 's' : ''})`)
    );
  }

  if (alreadyExist.length > 0) {
    const metadata = await admin.fetchTopicMetadata({ topics: alreadyExist.map(t => t.topic) });
    const toIncrease = [];

    metadata.topics.forEach(topicMeta => {
      const desired = TOPICS.find(t => t.topic === topicMeta.name).numPartitions;
      const actual = topicMeta.partitions.length;

      if (actual < desired) {
        toIncrease.push({ topic: topicMeta.name, count: desired });
      } else if (actual > desired) {
        console.log(`⚠️  Topic already exists with ${actual} partitions: ${topicMeta.name} (expected ${desired}). Cannot decrease partitions.`);
      }
    });

    if (toIncrease.length > 0) {
      await admin.createPartitions({ topicPartitions: toIncrease });
      toIncrease.forEach(t =>
        console.log(`🔧 Increased partitions for ${t.topic} to ${t.count}`)
      );
    }
  }

  await admin.disconnect();
}

setup().catch(err => {
  console.error('❌ Failed to create topics:', err.message);
  process.exit(1);
});
