const { Kafka } = require('kafkajs');

// Demo 7b: At-Least-Once Delivery
// Process FIRST, then commit
// If crash happens after processing but before commit, message is re-delivered (duplicate)

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-at-least-once', brokers: [BROKER] });
const producer = kafka.producer();
const TOPIC = 'delivery-at-least-once-demo';
const GROUP_ID = `at-least-once-group-${Date.now()}`;

async function seedMessages() {
  await producer.connect();
  console.log('  Seeding 5 test messages...\n');

  for (let i = 1; i <= 5; i++) {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify({ orderId: 100 + i, item: `Paneer Tikka x${i}`, amount: i * 150 }) }]
    });
  }

  await producer.disconnect();
}

async function firstRun() {
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  let count = 0;
  await new Promise((resolve, reject) => {
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        count++;
        const offset = message.offset;
        const o = JSON.parse(message.value.toString());

        console.log(`  📦 Processed: Order #${o.orderId} | ${o.item} | ₹${o.amount}`);

        if (count === 3) {
          console.log(`\n  💥 CRASH after processing Order #${o.orderId} but BEFORE commit!`);
          console.log('     Offset NOT committed. On restart, this same message is re-delivered.\n');
          await consumer.disconnect();
          resolve();
          return;
        }

        await consumer.commitOffsets([{ topic, partition, offset: (parseInt(offset) + 1).toString() }]);
        console.log(`  ✅ Offset ${offset} committed`);
      }
    }).catch(reject);
  });
}

async function restartRun() {
  console.log('  🔄 Restarting consumer with the same group...\n');

  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  let count = 0;
  await new Promise((resolve, reject) => {
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        count++;
        const offset = message.offset;
        const o = JSON.parse(message.value.toString());

        console.log(`  📦 Re-processed: Order #${o.orderId} | ${o.item} | ₹${o.amount}`);
        await consumer.commitOffsets([{ topic, partition, offset: (parseInt(offset) + 1).toString() }]);
        console.log(`  ✅ Offset ${offset} committed`);

        if (count === 3) {
          console.log('\n  Result: At-least-once = message may be duplicated, never lost.');
          console.log('  Fix: Make your consumer idempotent (see idempotent-consumer.js)');
          await consumer.disconnect();
          resolve();
        }
      }
    }).catch(reject);
  });
}

async function start() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 7b: At-Least-Once Delivery');
  console.log('  Strategy: Process FIRST, then commit');
  console.log('═══════════════════════════════════════════════════════════\n');

  await seedMessages();
  await firstRun();
  await restartRun();
  process.exit(0);
}

start().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
