const { Kafka } = require('kafkajs');

// Demo 7a: At-Most-Once Delivery
// Commit offset BEFORE processing
// If crash happens during processing, message is lost forever

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-at-most-once', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'at-most-once-group' });
const producer = kafka.producer();

async function start() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'delivery-guarantee-demo', fromBeginning: true });

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 7a: At-Most-Once Delivery');
  console.log('  Strategy: Commit FIRST, then process');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Seed some test messages
  console.log('  Seeding 5 test messages...\n');
  for (let i = 1; i <= 5; i++) {
    await producer.send({
      topic: 'delivery-guarantee-demo',
      messages: [{ value: JSON.stringify({ orderId: i, item: `Butter Chicken x${i}`, amount: i * 200 }) }]
    });
  }
  await producer.disconnect();

  let count = 0;
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      count++;
      const offset = message.offset;
      const o = JSON.parse(message.value.toString());

      // Step 1: COMMIT FIRST (before processing)
      await consumer.commitOffsets([{ topic, partition, offset: (parseInt(offset) + 1).toString() }]);
      console.log(`  ✅ Offset ${offset} committed`);

      // Step 2: Simulate crash on message 3
      if (count === 3) {
        console.log(`  💥 CRASH while processing Order #${o.orderId}!`);
        console.log('     Offset was already committed. This message is LOST.');
        console.log('     On restart, consumer will pick up from offset 3.\n');
        console.log('  Result: At-most-once = message may be lost, never duplicated.');
        await consumer.disconnect();
        process.exit(0);
      }

      // Step 3: Process (only reached if no crash)
      console.log(`  📦 Processed: Order #${o.orderId} | ${o.item} | ₹${o.amount}`);
    }
  });
}

start().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
