import { Kafka } from 'kafkajs';
import { WebSocket } from 'ws';
import moment from 'moment';

const kafka = new Kafka({
    clientId: 'my-producer',
    brokers: ['localhost:9092']
});

const products = ["BTC-USD"];

const producer = kafka.producer();

const runProducer = async () => {
    await producer.connect();
    console.log("Connected to Kafka");

    const ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');
    await new Promise(resolve => ws.once('open', resolve));
    console.log("Connected to Coinbase WebSocket");

    ws.send(JSON.stringify({
        "type": "subscribe",
        "product_ids": [
            ...products
        ],
        "channels": [
            {
                "name": "ticker",
                "product_ids": [
                    ...products
                ]
            }
        ]
    }));


    ws.on('message', async msg => {
        const messageString = msg.toString();
        console.log("Received message:", messageString);

        await producer.send({
            topic: "test",
            messages: [
                {
                    value: msg
                }
            ]
        });
    });

    ws.on('close', () => {
        console.log("WebSocket closed. Attempting to reconnect...");
        runProducer(); // Reconnect logic
    });

    ws.on('error', (err) => {
        console.error("WebSocket error:", err);
    });
};

runProducer().catch(console.error);