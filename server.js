// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json()); // Add this line to parse JSON request bodies if you ever need them

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// 1. Endpoint to get all districts
app.get('/api/districts', async (req, res) => {
  try {
    const query = `SELECT id, name FROM public.districts ORDER BY name;`;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Database error fetching districts:', err);
    res.status(500).json({ error: 'Failed to fetch districts' });
  }
});

// 2. Endpoint to get substations for a given district_id
app.get('/api/substations', async (req, res) => {
  const { district_id } = req.query; // Get district_id from query parameters

  if (!district_id) {
    return res.status(400).json({ error: 'district_id is required' });
  }

  try {
    const query = `SELECT id, name, district_id FROM public.substations WHERE district_id = $1 ORDER BY name;`;
    const result = await pool.query(query, [district_id]);
    res.json(result.rows);
  } catch (err) {
    console.error('Database error fetching substations:', err);
    res.status(500).json({ error: 'Failed to fetch substations' });
  }
});

// 3. Endpoint to get feeders for a given substation_id
app.get('/api/feeders', async (req, res) => {
  const { substation_id } = req.query; // Get substation_id from query parameters

  if (!substation_id) {
    return res.status(400).json({ error: 'substation_id is required' });
  }

  try {
    const query = `SELECT id, name, substation_id FROM public.feeders WHERE substation_id = $1 ORDER BY name;`;
    const result = await pool.query(query, [substation_id]);
    res.json(result.rows);
  } catch (err) {
    console.error('Database error fetching feeders:', err);
    res.status(500).json({ error: 'Failed to fetch feeders' });
  }
});


// 4. Modified /api/energy endpoint to accept filter parameters
app.get('/api/energy_data', async (req, res) => { // Renamed for clarity as it's specific data
  const { district_id, substation_id, feeder_id } = req.query; // Get filter IDs from query parameters

  let query = `
    WITH consumer_metrics AS (
        SELECT dtr_id, COUNT(consumer_no) AS consumer_count, SUM(load_kw) AS total_load_kw
        FROM public.consumers
        GROUP BY dtr_id
    ),
    dtr_with_metrics AS (
        SELECT dtrs.id AS dtr_id, dtrs.name AS dtr_name, dtrs.feeder_id,
             consumer_metrics.consumer_count, consumer_metrics.total_load_kw
        FROM public.dtrs
        LEFT JOIN consumer_metrics ON dtrs.id = consumer_metrics.dtr_id
    ),
    feeder_with_metrics AS (
        SELECT feeders.id AS feeder_id, feeders.name AS feeder_name, feeders.substation_id,
             COALESCE(SUM(dtr_with_metrics.consumer_count), 0) AS consumer_count,
             COALESCE(SUM(dtr_with_metrics.total_load_kw), 0) AS total_load_kw
        FROM public.feeders
        LEFT JOIN dtr_with_metrics ON feeders.id = dtr_with_metrics.feeder_id
        GROUP BY feeders.id, feeders.name, feeders.substation_id
    ),
    substation_with_metrics AS (
        SELECT substations.id AS substation_id, substations.name AS substation_name, substations.district_id,
             COALESCE(SUM(feeder_with_metrics.consumer_count), 0) AS consumer_count,
             COALESCE(SUM(feeder_with_metrics.total_load_kw), 0) AS total_load_kw
        FROM public.substations
        LEFT JOIN feeder_with_metrics ON substations.id = feeder_with_metrics.substation_id
        GROUP BY substations.id, substations.name, substations.district_id
    )
    SELECT districts.name AS district_name, substations.name AS substation_name,
           feeders.name AS feeder_name, dtrs.name AS dtr_name,
           COALESCE(consumer_metrics.consumer_count, 0) AS dtr_consumer_count,
           COALESCE(consumer_metrics.total_load_kw, 0) AS dtr_total_load_kw,
           COALESCE(feeder_with_metrics.consumer_count, 0) AS feeder_consumer_count,
           COALESCE(feeder_with_metrics.total_load_kw, 0) AS feeder_total_load_kw,
           COALESCE(substation_with_metrics.consumer_count, 0) AS substation_consumer_count,
           COALESCE(substation_with_metrics.total_load_kw, 0) AS substation_total_load_kw
    FROM public.dtrs
    LEFT JOIN consumer_metrics ON dtrs.id = consumer_metrics.dtr_id
    LEFT JOIN public.feeders ON dtrs.feeder_id = feeders.id
    LEFT JOIN feeder_with_metrics ON feeders.id = feeder_with_metrics.feeder_id
    LEFT JOIN public.substations ON feeders.substation_id = substations.id
    LEFT JOIN substation_with_metrics ON substations.id = substation_with_metrics.substation_id
    LEFT JOIN public.districts ON substations.district_id = districts.id
  `;

  const queryParams = [];
  let whereClauses = [];

  // Add WHERE clauses based on provided filter parameters
  if (district_id) {
    whereClauses.push('public.districts.id = $1');
    queryParams.push(district_id);
  }
  if (substation_id) {
    // If district_id is already present, substation_id will be $2, otherwise $1
    whereClauses.push(`public.substations.id = $${queryParams.length + 1}`);
    queryParams.push(substation_id);
  }
  if (feeder_id) {
    // Similarly for feeder_id
    whereClauses.push(`public.feeders.id = $${queryParams.length + 1}`);
    queryParams.push(feeder_id);
  }

  if (whereClauses.length > 0) {
    query += ' WHERE ' + whereClauses.join(' AND ');
  }

  query += ' ORDER BY district_name, substation_name, feeder_name, dtr_name;';

  try {
    const result = await pool.query(query, queryParams);
    res.json(result.rows);
  } catch (err) {
    console.error('Database error fetching filtered energy data:', err);
    res.status(500).json({ error: 'Failed to fetch filtered energy data' });
  }
});

app.listen(port, () => {
  console.log(`✅ Server running on http://localhost:${port}`);
});
