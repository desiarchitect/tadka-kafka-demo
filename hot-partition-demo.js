const { Kafka } = require('kafkajs');

// Demo 6: Hot Partition Problem
// 70% orders from Mumbai → one partition gets hammered
// Then fix with compound keys

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-hot-partition', brokers: [BROKER] });
const producer = kafka.producer();
const admin = kafka.admin();

const partitionCounts = { 0: 0, 1: 0, 2: 0 };

async function run() {
  await producer.connect();
  await admin.connect();

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  DEMO 6: Hot Partition Problem');
  console.log('  Topic: order-events (3 partitions)');
  console.log('═══════════════════════════════════════════════════════════\n');

  // Part 1: Skewed distribution — 70% Mumbai
  console.log('── Part 1: Skewed key distribution (70% Mumbai) ──\n');

  const skewedCities = [];
  // 70% mumbai, 10% delhi, 10% bangalore, 10% pune
  for (let i = 0; i < 70; i++) skewedCities.push('mumbai');
  for (let i = 0; i < 10; i++) skewedCities.push('delhi');
  for (let i = 0; i < 10; i++) skewedCities.push('bangalore');
  for (let i = 0; i < 10; i++) skewedCities.push('pune');

  partitionCounts[0] = 0;
  partitionCounts[1] = 0;
  partitionCounts[2] = 0;

  for (let i = 0; i < 100; i++) {
    const city = skewedCities[i];
    const result = await producer.send({
      topic: 'order-events',
      messages: [{
        key: city,
        value: JSON.stringify({ orderId: 5000 + i, city, amount: 300 })
      }]
    });
    partitionCounts[result[0].partition]++;
  }

  console.log('  100 orders sent with 70% Mumbai:\n');
  for (const [p, count] of Object.entries(partitionCounts)) {
    const bar = '█'.repeat(Math.floor(count / 2));
    const pct = count + '%';
    const hot = count > 40 ? ' 🔥 HOT PARTITION!' : '';
    console.log(`  Partition ${p}: ${bar} ${pct}${hot}`);
  }

  console.log('\n  ⚠️  One partition is handling most of the load!');
  console.log('     Consumer on that partition will lag. Others sit idle.\n');

  // Part 2: Fix with compound keys
  console.log('── Part 2: Fix with compound keys (mumbai_1, mumbai_2, mumbai_3) ──\n');

  // Create a separate topic for the fix demo
  await admin.createTopics({
    topics: [{ topic: 'hot-partition-fix', numPartitions: 3, replicationFactor: 1 }]
  }).catch(() => {});

  const fixedCounts = { 0: 0, 1: 0, 2: 0 };

  for (let i = 0; i < 100; i++) {
    const baseCities = skewedCities[i];
    // Compound key: append random suffix to spread Mumbai across partitions
    const suffix = Math.floor(Math.random() * 3) + 1;
    const compoundKey = baseCities === 'mumbai' ? `mumbai_${suffix}` : baseCities;

    const result = await producer.send({
      topic: 'hot-partition-fix',
      messages: [{
        key: compoundKey,
        value: JSON.stringify({ orderId: 6000 + i, city: baseCities, amount: 300, key: compoundKey })
      }]
    });
    fixedCounts[result[0].partition]++;
  }

  console.log('  100 orders sent with compound keys (mumbai_1, mumbai_2, mumbai_3):\n');
  for (const [p, count] of Object.entries(fixedCounts)) {
    const bar = '█'.repeat(Math.floor(count / 2));
    const pct = count + '%';
    console.log(`  Partition ${p}: ${bar} ${pct}`);
  }

  console.log('\n  ✅ Load is now much more evenly distributed!');
  console.log('     Compound keys spread hot-city orders across multiple partitions.');
  console.log('     Trade-off: you lose per-city ordering (but gain throughput).\n');

  await producer.disconnect();
  await admin.disconnect();
}

run().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
