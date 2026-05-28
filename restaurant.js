const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-restaurant', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'restaurant-service' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
  console.log('🍽 Restaurant Service started — alerting restaurants...\n');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const o = JSON.parse(message.value.toString());
      console.log(`🍽  Restaurant alert: Order #${o.orderId} | ${o.city} | ${o.restaurantId}`);
    }
  });
}

start().catch(err => {
  console.error('❌ Restaurant Service error:', err.message);
  process.exit(1);
});
