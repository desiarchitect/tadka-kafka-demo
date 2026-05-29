const { Kafka } = require('kafkajs');

// Demo 4: Partition Key & Ordering
// Shows how hash(key) % numPartitions routes messages to partitions
// Same key = same partition = ordering guaranteed

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-partition-demo', brokers: [BROKER] });
const producer = kafka.producer();
const admin = kafka.admin();

async function run() {
  await producer.connect();
  await admin.connect();

  // Ensure topic exists
  await admin.createTopics({
    topics: [{ topic: 'partition-demo', numPartitions: 3, replicationFactor: 1 }]
  }).catch(() => {});

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 4: Partition Key & Ordering');
  console.log('  Topic: partition-demo (3 partitions)');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Part 1: Same key → same partition
  console.log('── Part 1: Same key (mumbai) → All go to same partition ──\n');

  for (let i = 1; i <= 8; i++) {
    const result = await producer.send({
      topic: 'partition-demo',
      messages: [{
        key: 'mumbai',
        value: JSON.stringify({ orderId: i, city: 'mumbai', amount: i * 100 })
      }]
    });
    const { partition, baseOffset } = result[0];
    console.log(`  Order #${i} | key=mumbai → Partition ${partition} | Offset ${baseOffset}`);
  }

  console.log('\n  ✅ Notice: ALL messages with key "mumbai" went to the SAME partition');
  console.log('     Ordering is guaranteed within this partition.\n');

  // Part 2: Different keys → distributed across partitions
  console.log('── Part 2: Different keys → Distributed across partitions ──\n');

  const cities = ['mumbai', 'delhi', 'bangalore', 'pune', 'hyderabad', 'chennai', 'kolkata', 'jaipur'];

  for (const city of cities) {
    const result = await producer.send({
      topic: 'partition-demo',
      messages: [{
        key: city,
        value: JSON.stringify({ orderId: 9000 + cities.indexOf(city), city, amount: 500 })
      }]
    });
    const { partition, baseOffset } = result[0];
    console.log(`  key="${city}" → Partition ${partition} | Offset ${baseOffset}`);
  }

  console.log('\n  ✅ Different keys are distributed via hash(key) % 3');
  console.log('     Same city always lands in the same partition.\n');

  // Part 3: Show partition distribution
  console.log('── Part 3: Partition-wise message count ──\n');

  const topicOffsets = await admin.fetchTopicOffsets('partition-demo');
  for (const p of topicOffsets) {
    console.log(`  Partition ${p.partition}: ${p.offset} messages (offset range 0 → ${p.offset})`);
  }

  console.log('\n  💡 Key insight: Ordering is ONLY guaranteed within a partition.');
  console.log('     Across partitions, no ordering guarantee.\n');

  await producer.disconnect();
  await admin.disconnect();
}

run().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
