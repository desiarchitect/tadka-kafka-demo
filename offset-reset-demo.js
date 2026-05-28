const { Kafka } = require('kafkajs');

// Demo 8: Offset Reset & Replay
// Shows the power of Kafka's log — replay ALL messages from the beginning

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-offset-reset', brokers: [BROKER] });
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId: 'offset-reset-demo-group' });

async function run() {
  await admin.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: true });

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 8: Offset Reset & Replay');
  console.log('  Group: offset-reset-demo-group | Topic: order-events');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Phase 1: Consume normally (first 10 messages)
  console.log('── Phase 1: Normal consumption (first 10 messages) ──\n');

  let count = 0;
  const phase1Promise = new Promise((resolve) => {
    consumer.run({
      eachMessage: async ({ partition, message }) => {
        count++;
        const o = JSON.parse(message.value.toString());
        console.log(`  [${count}] Order #${o.orderId} | ${o.city} | P${partition} | Offset ${message.offset}`);

        if (count === 10) {
          resolve();
        }
      }
    });
  });

  await phase1Promise;
  console.log('\n  ✅ Processed 10 messages normally.');
  console.log('     Current offset = 10 (consumer remembers where it stopped)\n');

  // Phase 2: Reset offset to beginning
  console.log('── Phase 2: Resetting offset to BEGINNING ──\n');
  console.log('  🔄 Resetting all partition offsets to 0...\n');

  // Stop consumer, reset offsets, restart
  await consumer.stop();

  // Get partitions
  const topicOffsets = await admin.fetchTopicOffsets('order-events');
  const resetOffsets = topicOffsets.map(p => ({
    partition: p.partition,
    offset: '0'  // Reset to beginning!
  }));

  await admin.setOffsets({
    groupId: 'offset-reset-demo-group',
    topic: 'order-events',
    partitions: resetOffsets
  });

  console.log('  ✅ Offsets reset to 0 for all partitions!');
  console.log('     Now replaying ALL messages from the very beginning...\n');

  // Phase 3: Replay from beginning
  console.log('── Phase 3: Full replay from offset 0 ──\n');

  const consumer2 = kafka.consumer({ groupId: 'offset-reset-demo-group' });
  await consumer2.connect();
  await consumer2.subscribe({ topic: 'order-events', fromBeginning: true });

  let replayCount = 0;
  await consumer2.run({
    eachMessage: async ({ partition, message }) => {
      replayCount++;
      const o = JSON.parse(message.value.toString());
      if (replayCount <= 10) {
        console.log(`  [REPLAY ${replayCount}] Order #${o.orderId} | ${o.city} | P${partition} | Offset ${message.offset}`);
      }
      if (replayCount === 10) {
        console.log(`  ... (showing first 10 of replay)\n`);
        console.log('  ✅ Full replay complete! Same messages, consumed again.');
        console.log('     Use case: Bug fix, new analytics pipeline, audit trail.');
        console.log('     This is impossible with RabbitMQ — message is gone after ACK.\n');
        console.log('  💡 Kafka = replayable log. RabbitMQ = fire-and-forget queue.');
        await consumer2.disconnect();
        await admin.disconnect();
        await consumer.disconnect();
        process.exit(0);
      }
    }
  });
}

run().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
