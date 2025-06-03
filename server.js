// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

app.get('/api/energy', async (req, res) => {
  try {
    const query = `
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
      ORDER BY district_name, substation_name, feeder_name, dtr_name;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Database error:', err);
    res.status(500).json({ error: 'Failed to fetch energy data' });
  }
});

app.listen(port, () => {
  console.log(`✅ Server running on http://localhost:${port}`);
});
