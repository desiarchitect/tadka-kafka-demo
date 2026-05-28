const { Kafka } = require('kafkajs');

// Demo 5: Consumer Scaling — The Golden Rule
// Max useful consumers in a group = number of partitions
// Run multiple instances: node scaling-demo.js 1, node scaling-demo.js 2, etc.

const instanceId = process.argv[2] || '1';
const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: `tadka-scaler-${instanceId}`, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'scaling-demo-group' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: false });

  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  DEMO 5: Consumer Scaling — Instance #${instanceId}`);
  console.log('  Group: scaling-demo-group | Topic: order-events (3 partitions)');
  console.log('═══════════════════════════════════════════════════════════\n');

  const assignedPartitions = [];
  let idleTimer = null;

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    // Clear any pending idle warning — a new GROUP_JOIN means rebalance is still happening
    if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }

    const members = payload.memberAssignment || {};
    assignedPartitions.length = 0;

    if (members['order-events']) {
      assignedPartitions.push(...members['order-events']);
    }

    if (assignedPartitions.length > 0) {
      console.log(`  ✅ Instance #${instanceId}: Assigned partitions → [${assignedPartitions.join(', ')}]`);
      console.log(`     Processing ${assignedPartitions.length} of 3 partitions\n`);
    } else {
      // Empty on first join = rebalance still in flight; wait for the next GROUP_JOIN.
      // Only show IDLE warning if still unassigned after 3 seconds.
      idleTimer = setTimeout(() => {
        if (assignedPartitions.length === 0) {
          console.log(`  ⚠️  Instance #${instanceId}: NO partitions assigned — IDLE consumer!`);
          console.log('     Golden rule: max consumers = number of partitions (3)');
          console.log('     This instance is wasting resources.\n');
        }
      }, 3000);
    }
  });

  consumer.on(consumer.events.REBALANCING, () => {
    console.log(`  🔄 Instance #${instanceId}: Rebalancing in progress...`);
  });

  let messageCount = 0;
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      messageCount++;
      const o = JSON.parse(message.value.toString());
      if (messageCount <= 5 || messageCount % 10 === 0) {
        console.log(`  [Instance #${instanceId}] Order #${o.orderId} | ${o.city} | P${partition}`);
      }
    }
  });
}

start().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log(`\n  🛑 Instance #${instanceId} leaving group — triggers rebalance for others`);
  await consumer.disconnect();
  process.exit(0);
});
