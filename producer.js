const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({
  clientId: 'tadka-order-service',
  brokers: [BROKER]
});
const producer = kafka.producer();
const cities = ['mumbai', 'delhi', 'bangalore', 'pune'];

async function sendOrders() {
  await producer.connect();
  console.log('🚀 Tadka Order Service connected to Kafka');
  console.log('   Sending orders every 1 second...\n');

  let id = 1001;
  setInterval(async () => {
    const city = cities[Math.floor(Math.random() * cities.length)];
    const order = {
      orderId: id++,
      city,
      amount: Math.floor(Math.random() * 800) + 100,
      restaurantId: `rest_${Math.floor(Math.random() * 50) + 1}`,
      timestamp: new Date().toISOString()
    };
    await producer.send({
      topic: 'order-events',
      messages: [{ key: city, value: JSON.stringify(order) }]
    });
    console.log(`📦 Order placed: #${order.orderId} | ${city} | ₹${order.amount}`);
  }, 1000);
}

sendOrders().catch(err => {
  console.error('❌ Producer error:', err.message);
  process.exit(1);
});
