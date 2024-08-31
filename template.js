// producer.js
import { Kafka } from 'kafkajs';
import { randFloat } from '@ngneat/falso';
import moment from 'moment';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();

  var dateCounter = moment();

  while(true) {
    const value = JSON.stringify({ 
      timestamp: dateCounter,
      
    });

    await producer.send({
      topic: 'test',
      messages: [
        { value: value }
      ],
    });

    console.log(value);

    // Simulate a delay (e.g., 1 second)
    await new Promise(resolve => setTimeout(resolve, 1000));
    dateCounter = moment(dateCounter).add(1, 'minutes');
  }

  await producer.disconnect();
};

runProducer().catch(console.error);