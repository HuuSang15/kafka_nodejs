const express = require("express");
const kafka = require("kafka-node");
const app = express();
const mongoose = require('mongoose');

app.use(express.json());

const dbsAreRunning  = async () => {
  try {
    const userSchema = mongoose.Schema({
      name: String,
      email: String,
      password: String,
    });
    const User = mongoose.model('User', userSchema);
    
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGO_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    console.log("Connected to MongoDB");

    // Create Kafka Client and Consumer
    const client = new kafka.KafkaClient({
      kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
    });
    const consumer = new kafka.Consumer(client, [{topic: process.env.KAFKA_TOPIC}], {
      autoCommit: false,
    });

    consumer.on('message', async (message) => {
      try {
        const user = new User(JSON.parse(message.value));
        await user.save();
        console.log("Message saved to MongoDB");
      } catch (error) {
        console.error("Error saving message to MongoDB:", error);
      }
    });

    consumer.on('error', (err) => {
      console.error("Kafka Consumer Error:", err);
    });
  } catch (error) {
    console.error("Error connecting to MongoDB or Kafka:", error);
  }
};

// Delay the execution to ensure services are up and running
setTimeout(dbsAreRunning, 10000);
app.listen(process.env.PORT);