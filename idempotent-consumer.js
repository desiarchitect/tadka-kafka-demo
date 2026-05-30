const { Kafka } = require('kafkajs');

// Demo 7c: Idempotent Consumer
// Process + dedup using orderId before commit
// In production use Redis or DB, here in-memory Set for demo

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-idempotent', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'idempotent-consumer-group' });
const producer = kafka.producer();
const TOPIC = 'idempotent-consumer-demo';

// In-memory dedup set (In production use Redis or DB)
const processedOrders = new Set();

async function start() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 7c: Idempotent Consumer');
  console.log('  Strategy: Check if already processed → skip duplicates');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Seed messages with intentional duplicates
  console.log('  Seeding 7 messages (with duplicates)...\n');
  const orders = [
    { orderId: 201, item: 'Dal Makhani', amount: 350 },
    { orderId: 202, item: 'Naan x4', amount: 120 },
    { orderId: 201, item: 'Dal Makhani', amount: 350 },  // duplicate!
    { orderId: 203, item: 'Biryani', amount: 400 },
    { orderId: 202, item: 'Naan x4', amount: 120 },      // duplicate!
    { orderId: 204, item: 'Gulab Jamun x2', amount: 100 },
    { orderId: 201, item: 'Dal Makhani', amount: 350 }   // triple!
  ];

  for (const order of orders) {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(order) }]
    });
  }
  await producer.disconnect();

  let totalReceived = 0;
  let duplicatesSkipped = 0;

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      totalReceived++;
      const o = JSON.parse(message.value.toString());

      // Idempotency check
      if (processedOrders.has(o.orderId)) {
        duplicatesSkipped++;
        console.log(`  ⏭️  SKIP duplicate: Order #${o.orderId} (already processed)`);
      } else {
        processedOrders.add(o.orderId);
        console.log(`  ✅ Processed: Order #${o.orderId} | ${o.item} | ₹${o.amount}`);
      }

      // Commit after dedup check
      await consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset) + 1).toString() }]);

      if (totalReceived === orders.length) {
        console.log('\n  ═══ Summary ═══');
        console.log(`  Total received:     ${totalReceived}`);
        console.log(`  Unique processed:   ${processedOrders.size}`);
        console.log(`  Duplicates skipped: ${duplicatesSkipped}`);
        console.log('\n  💡 In production, use Redis SET or DB unique constraint for dedup.');
        console.log('     Pattern: IF NOT EXISTS → INSERT → COMMIT');
        setTimeout(async () => {
          await consumer.disconnect();
          process.exit(0);
        }, 1000);
      }
    }
  });
}

start().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
