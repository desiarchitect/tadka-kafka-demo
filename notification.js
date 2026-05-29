const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'tadka-notif', brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'notification-service' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
  console.log('📱 Notification Service started, waiting for orders...\n');

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const o = JSON.parse(message.value.toString());
      console.log(`📱 SMS sent: Order #${o.orderId} confirmed! ₹${o.amount} | [P${partition}]`);
    }
  });
}

start().catch(err => {
  console.error('❌ Notification Service error:', err.message);
  process.exit(1);
});
