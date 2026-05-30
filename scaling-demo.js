const { Kafka } = require('kafkajs');

// Demo 5: Consumer Scaling - The Golden Rule
// Max useful consumers in a group = number of partitions
// Run multiple instances: node scaling-demo.js 1, node scaling-demo.js 2, etc.

const instanceId = process.argv[2] || '1';
const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const topic = 'order-events';
const groupId = 'scaling-demo-group';
const kafka = new Kafka({ clientId: `tadka-scaler-${instanceId}`, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId });

async function getTopicPartitionCount(topicName) {
  const admin = kafka.admin();
  await admin.connect();
  const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
  await admin.disconnect();

  const topicMeta = metadata.topics.find(t => t.name === topicName);
  if (!topicMeta) {
    throw new Error(`Topic ${topicName} does not exist`);
  }

  return topicMeta.partitions.length;
}

async function start() {
  const partitionCount = await getTopicPartitionCount(topic);

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  DEMO 5: Consumer Scaling - Instance #${instanceId}`);
  console.log(`  Group: ${groupId} | Topic: ${topic} (${partitionCount} partition${partitionCount > 1 ? 's' : ''})`);
  console.log('═══════════════════════════════════════════════════════════\n');

  const assignedPartitions = [];
  let idleTimer = null;

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    // Clear any pending idle warning; a new GROUP_JOIN means rebalance is still happening.
    if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }

    const members = payload.memberAssignment || {};
    assignedPartitions.length = 0;

    if (Array.isArray(members[topic])) {
      assignedPartitions.push(...members[topic]);
    }

    if (assignedPartitions.length > 0) {
      console.log(`  ✅ Instance #${instanceId}: Assigned partitions → [${assignedPartitions.join(', ')}]`);
      console.log(`     Processing ${assignedPartitions.length} of ${partitionCount} partition${partitionCount > 1 ? 's' : ''}\n`);
    } else {
      idleTimer = setTimeout(() => {
        if (assignedPartitions.length === 0) {
          console.log(`  ⚠️  Instance #${instanceId}: NO partitions assigned, IDLE consumer!`);
          console.log(`     Golden rule: max consumers = number of partitions (${partitionCount})`);
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
  console.log(`\n  🛑 Instance #${instanceId} leaving group, triggers rebalance for others`);
  await consumer.disconnect();
  process.exit(0);
});
