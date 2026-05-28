const { Kafka } = require('kafkajs');

// Demo 7b: At-Least-Once Delivery
// Process FIRST, then commit
// If crash happens after processing but before commit — message is re-delivered (duplicate)

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-at-least-once', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'at-least-once-group' });
const producer = kafka.producer();

async function start() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'delivery-guarantee-demo', fromBeginning: true });

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 7b: At-Least-Once Delivery');
  console.log('  Strategy: Process FIRST, then commit');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Seed some test messages
  console.log('  Seeding 5 test messages...\n');
  for (let i = 1; i <= 5; i++) {
    await producer.send({
      topic: 'delivery-guarantee-demo',
      messages: [{ value: JSON.stringify({ orderId: 100 + i, item: `Paneer Tikka x${i}`, amount: i * 150 }) }]
    });
  }
  await producer.disconnect();

  let count = 0;
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      count++;
      const offset = message.offset;
      const o = JSON.parse(message.value.toString());

      // Step 1: PROCESS FIRST
      console.log(`  📦 Processed: Order #${o.orderId} | ${o.item} | ₹${o.amount}`);

      // Step 2: Simulate crash BEFORE commit on message 3
      if (count === 3) {
        console.log(`\n  💥 CRASH after processing Order #${o.orderId} but BEFORE commit!`);
        console.log('     Offset NOT committed — on restart, message 3 will be re-delivered.');
        console.log('     You get a DUPLICATE. Customer gets charged twice? 😱\n');
        console.log('  Result: At-least-once = message may be duplicated, never lost.');
        console.log('  Fix: Make your consumer idempotent (see idempotent-consumer.js)');
        await consumer.disconnect();
        process.exit(0);
      }

      // Step 3: COMMIT AFTER processing
      await consumer.commitOffsets([{ topic, partition, offset: (parseInt(offset) + 1).toString() }]);
      console.log(`  ✅ Offset ${offset} committed`);
    }
  });
}

start().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
