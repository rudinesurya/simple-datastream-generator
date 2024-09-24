// producer.js
import { MongoClient } from 'mongodb';
import { seed, randFloat, randNumber } from '@ngneat/falso';
import moment from 'moment';

const url = 'mongodb://localhost:27017'; // MongoDB server URL
const dbName = 'TEST'; // Database name
const collectionName = 'readings_test'; // Collection name
const startDate = '2024-08-25T00:00:00Z';

const client = new MongoClient(url);

const runProducer = async () => {
  seed('nkasdjnqjwenj');
  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    
    // Clear the collection on start
    await collection.deleteMany({});
    console.log(`Cleared collection: ${collectionName}`);

    let dateCounter = moment(startDate);

    while (true) {
      const document = {
        timestamp: dateCounter.toDate(),
        accountId: randNumber({ min:1, max:5 }),
        amount: randFloat({ min: 0.1, max: 500, fraction: 1 })
      };

      await collection.insertOne(document);

      console.log('Inserted document:', document);

      // Simulate a delay (e.g., 1 second)
      await new Promise(resolve => setTimeout(resolve, 1000));
      dateCounter = dateCounter.add(1, 'minutes');
    }
  } catch (error) {
    console.error('Producer error:', error);
  } finally {
    await client.close();
    console.log('Disconnected from MongoDB');
  }
};

runProducer();
