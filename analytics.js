const { Kafka } = require('kafkajs');

// Production mein: yeh ClickHouse mein insert karega
const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-analytics', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'analytics-service' });
const revenue = {};

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
  console.log('📊 Analytics Service started — tracking revenue for ClickHouse...\n');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const o = JSON.parse(message.value.toString());
      revenue[o.city] = (revenue[o.city] || 0) + o.amount;
      console.log(`📊 [ClickHouse] ${o.city} revenue: ₹${revenue[o.city]}`);
    }
  });
}

start().catch(err => {
  console.error('❌ Analytics Service error:', err.message);
  process.exit(1);
});
