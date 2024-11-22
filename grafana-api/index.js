const express = require('express');
const cassandra = require('cassandra-driver');
const app = express();
const port = 3000;

// Cassandra client configuration
const client = new cassandra.Client({
  contactPoints: ['cassandra'], // This should be the service name from your Docker Compose
  localDataCenter: 'datacenter1', // Adjust based on your Cassandra setup
  keyspace: 'weather'
});

// Route to get all weather data
app.get('/weather', async (req, res) => {
  const query = 'SELECT * FROM weather_data';
  try {
    const result = await client.execute(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching data from Cassandra:', err);
    res.status(500).send('Error fetching data');
  }
});

// Start the Express server
app.listen(port, () => {
  console.log(`Weather API listening at http://localhost:${port}`);
});
