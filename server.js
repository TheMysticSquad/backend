require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const mysql = require('mysql2/promise');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// PostgreSQL Pool
const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// MySQL Pool for KPI
const parseMysqlUrl = (mysqlUrl) => {
  const parsed = new URL(mysqlUrl);
  return {
    host: parsed.hostname,
    port: parsed.port,
    user: parsed.username,
    password: parsed.password,
    database: parsed.pathname.replace('/', ''),
    ssl: { rejectUnauthorized: false }
    
  };
};

const mysqlPool = mysql.createPool(parseMysqlUrl(process.env.KPI_DB_URL));

/* ---------- Main Data Endpoints (PostgreSQL) ---------- */
app.get('/api/districts', async (req, res) => {
  try {
    const result = await pgPool.query(`SELECT id, name FROM public.districts ORDER BY name;`);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching districts:', err);
    res.status(500).json({ error: 'Failed to fetch districts' });
  }
});

app.get('/api/substations', async (req, res) => {
  const { district_id } = req.query;
  if (!district_id) return res.status(400).json({ error: 'district_id is required' });

  try {
    const result = await pgPool.query(
      `SELECT id, name, district_id FROM public.substations WHERE district_id = $1 ORDER BY name;`,
      [district_id]
    );
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching substations:', err);
    res.status(500).json({ error: 'Failed to fetch substations' });
  }
});

app.get('/api/feeders', async (req, res) => {
  const { substation_id } = req.query;
  if (!substation_id) return res.status(400).json({ error: 'substation_id is required' });

  try {
    const result = await pgPool.query(
      `SELECT id, name, substation_id FROM public.feeders WHERE substation_id = $1 ORDER BY name;`,
      [substation_id]
    );
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching feeders:', err);
    res.status(500).json({ error: 'Failed to fetch feeders' });
  }
});

app.get('/api/energy_data', async (req, res) => {
  const { district_id, substation_id, feeder_id } = req.query;

  let query = `-- your long WITH SQL block as-is (omitted for brevity)`;
  const queryParams = [];
  const whereClauses = [];

  if (district_id) {
    whereClauses.push(`public.districts.id = $${queryParams.length + 1}`);
    queryParams.push(district_id);
  }
  if (substation_id) {
    whereClauses.push(`public.substations.id = $${queryParams.length + 1}`);
    queryParams.push(substation_id);
  }
  if (feeder_id) {
    whereClauses.push(`public.feeders.id = $${queryParams.length + 1}`);
    queryParams.push(feeder_id);
  }

  if (whereClauses.length > 0) query += ` WHERE ${whereClauses.join(' AND ')}`;
  query += ` ORDER BY district_name, substation_name, feeder_name, dtr_name;`;

  try {
    const result = await pgPool.query(query, queryParams);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching energy data:', err);
    res.status(500).json({ error: 'Failed to fetch filtered energy data' });
  }
});

/* ---------- KPI Endpoints (MySQL with label/value formatting) ---------- */
const fetchKPI = (tableName, labelKey, valueKey) => async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(`SELECT * FROM ${tableName} ORDER BY id;`);
    const formatted = rows.map(row => ({
      label: row[labelKey],
      value: row[valueKey]
    }));
    res.json({ data: formatted });
  } catch (err) {
    console.error(`Error fetching KPI ${tableName}:`, err);
    res.status(500).json({ error: `Failed to fetch KPI: ${tableName}` });
  }
};

// Regular KPI endpoints with correct column names
app.get('/api/kpi/billing-efficiency', fetchKPI('billing_efficiency', 'month', 'value'));
app.get('/api/kpi/revenue-collection-efficiency', fetchKPI('revenue_collection_efficiency', 'month', 'value'));
app.get('/api/kpi/avg-billing-per-consumer', fetchKPI('avg_billing_per_consumer', 'month', 'value'));
app.get('/api/kpi/billing-rate', fetchKPI('billing_rate', 'region', 'rate'));
app.get('/api/kpi/collection-rate', fetchKPI('collection_rate', 'month', 'value'));
app.get('/api/kpi/unbilled-consumers', fetchKPI('unbilled_consumers', 'month', 'percent'));
app.get('/api/kpi/arrear-ratio', fetchKPI('arrear_ratio', 'month', 'percent'));
app.get('/api/kpi/billing-coverage', fetchKPI('billing_coverage', 'month', 'value'));

// Custom endpoints for special tables

app.get('/api/kpi/disputed-bills', async (req, res) => {
  try {
    const [rows] = await mysqlPool.query('SELECT * FROM disputed_bills ORDER BY id;');
    res.json({ data: rows });
  } catch (err) {
    console.error('Error fetching KPI disputed_bills:', err);
    res.status(500).json({ error: 'Failed to fetch KPI: disputed_bills' });
  }
});

app.get('/api/kpi/metering-status', async (req, res) => {
  try {
    const [rows] = await mysqlPool.query('SELECT * FROM metering_status ORDER BY id;');
    res.json({ data: rows });
  } catch (err) {
    console.error('Error fetching KPI metering_status:', err);
    res.status(500).json({ error: 'Failed to fetch KPI: metering_status' });
  }
});

/* ---------- Start Server ---------- */
app.listen(port, () => {
  console.log(`✅ Server running on http://localhost:${port}`);
});
