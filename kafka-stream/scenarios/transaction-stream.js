// producer.js
import { Kafka } from 'kafkajs';
import { randFloat, randNumber } from '@ngneat/falso';
import moment from 'moment';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();

  var dateCounter = moment();
  var id = 0;

  while(true) {
    const value = JSON.stringify({ 
      id: id,
      timestamp: dateCounter.valueOf(),
      accountId: randNumber({ min:1, max:5 }),
      amount: randFloat({ min: 0.1, max: 500, fraction: 1 })
    });

    await producer.send({
      topic: 'test-2',
      messages: [
        { value: value }
      ],
    });

    console.log(value);

    // Simulate a delay (e.g., 1 second)
    await new Promise(resolve => setTimeout(resolve, 1000));
    dateCounter = moment(dateCounter).add(1, 'minutes');
    id++;
  }

  await producer.disconnect();
};

runProducer().catch(console.error);