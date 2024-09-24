import { Kafka } from 'kafkajs';
import readline from 'readline';

// Initialize Kafka client and producer
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'] // Replace with your Kafka broker(s)
});

const producer = kafka.producer();

// Create a readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Function to send message to Kafka
const sendMessage = async (message) => {
  try {
    await producer.connect();
    await producer.send({
      topic: 'test', // Replace with your Kafka topic
      messages: [{ value: message }],
    });
    console.log(`Message sent: ${message}`);
  } catch (error) {
    console.error('Error sending message:', error);
  }
};

// Listen for user input and send to Kafka topic
rl.on('line', async (input) => {
  if (input === 'exit') {
    console.log('Exiting...');
    await producer.disconnect();
    process.exit(0); // Exit the application
  }

  // Send user input to Kafka
  await sendMessage(input);
});
