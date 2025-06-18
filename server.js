require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

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

const mysqlPool = mysql.createPool(parseMysqlUrl(process.env.DATABASE_URL));

// Logging middleware
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.originalUrl}`);
    next();
});

// Validation utility
const validateAndGetGeoName = async (id, tableName, idCol, nameCol, parentId = null, parentIdCol = null) => {
    if (id === undefined || id === null) return null;
    if (isNaN(parseInt(id)) || String(parseInt(id)) !== String(id)) {
        throw new Error(`Invalid ID format for ${idCol}: ${id}. Must be an integer.`);
    }
    const parsedId = parseInt(id);
    let query = `SELECT ${nameCol} FROM ${tableName} WHERE ${idCol} = ?`;
    const params = [parsedId];

    if (parentId !== null) {
        if (isNaN(parseInt(parentId)) || String(parseInt(parentId)) !== String(parentId)) {
            throw new Error(`Invalid parent ID format for ${parentIdCol}: ${parentId}. Must be an integer.`);
        }
        query += ` AND ${parentIdCol} = ?`;
        params.push(parseInt(parentId));
    }

    const [rows] = await mysqlPool.query(query, params);
    if (rows.length === 0) {
        throw new Error(`Invalid ${idCol}: ${id} does not exist${parentId ? ` or does not belong to ${parentIdCol}: ${parentId}` : ''}.`);
    }
    return rows[0][nameCol];
};

// Filters
app.get('/api/filters/circles', async (req, res) => {
    try {
        const [rows] = await mysqlPool.query(`SELECT CircleID AS id, CircleName AS name FROM Circles ORDER BY CircleName;`);
        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch circles', details: err.message });
    }
});

app.get('/api/filters/divisions', async (req, res) => {
    const { circleId } = req.query;
    try {
        if (!circleId) return res.status(400).json({ error: 'circleId is required.' });
        await validateAndGetGeoName(circleId, 'Circles', 'CircleID', 'CircleName');
        const [rows] = await mysqlPool.query(`SELECT DivisionID AS id, DivisionName AS name FROM Divisions WHERE CircleID = ? ORDER BY DivisionName;`, [circleId]);
        res.json(rows);
    } catch (err) {
        res.status(400).json({ error: err.message });
    }
});

app.get('/api/filters/subdivisions', async (req, res) => {
    const { divisionId } = req.query;
    try {
        if (!divisionId) return res.status(400).json({ error: 'divisionId is required.' });
        await validateAndGetGeoName(divisionId, 'Divisions', 'DivisionID', 'DivisionName');
        const [rows] = await mysqlPool.query(`SELECT SubdivisionID AS id, SubdivisionName AS name FROM Subdivisions WHERE DivisionID = ? ORDER BY SubdivisionName;`, [divisionId]);
        res.json(rows);
    } catch (err) {
        res.status(400).json({ error: err.message });
    }
});

app.get('/api/filters/sections', async (req, res) => {
    const { subdivisionId } = req.query;
    try {
        if (!subdivisionId) return res.status(400).json({ error: 'subdivisionId is required.' });
        await validateAndGetGeoName(subdivisionId, 'Subdivisions', 'SubdivisionID', 'SubdivisionName');
        const [rows] = await mysqlPool.query(`SELECT SectionID AS id, SectionName AS name FROM Sections WHERE SubdivisionID = ? ORDER BY SectionName;`, [subdivisionId]);
        res.json(rows);
    } catch (err) {
        res.status(400).json({ error: err.message });
    }
});

// KPI year filter
const KPI_TABLES = [
    'BillingCollectionSummary', 'ArrearAgingKPI', 'ConsumptionAnomalyKPI', 'DisconnectionReconnectionKPI',
    'DTHealthKPI', 'BreakdownRestorationKPI', 'ConnectionProgressKPI', 'FeederOutageKPI', 'ATCLossKPI',
    'GeoPerformanceSnapshot', 'billing_efficiency', 'revenue_collection_efficiency', 'avg_billing_per_consumer',
    'billing_rate', 'collection_rate', 'unbilled_consumers', 'arrear_ratio', 'billing_coverage'
];

app.get('/api/filters/years', async (req, res) => {
    try {
        const unionQueries = KPI_TABLES.map(table => `SELECT DISTINCT Year FROM ${table}`);
        const fullQuery = unionQueries.join(' UNION ALL ');
        const finalQuery = `SELECT DISTINCT Year FROM (${fullQuery}) AS CombinedYears ORDER BY Year DESC;`;
        const [rows] = await mysqlPool.query(finalQuery);
        res.json(rows.map(r => String(r.Year)));
    } catch (err) {
        res.status(500).json({
            error: "Database schema mismatch: 'Year' column missing in one or more KPI tables.",
            details: err.message,
            tablesChecked: KPI_TABLES
        });
    }
});

// Generic KPI handler (for non-monthly tables)
const fetchKPI = (table, columns) => async (req, res) => {
    const { sectionId, year } = req.query;

    try {
        if (!sectionId || !year) {
            return res.status(400).json({ error: 'Both sectionId and year are required.' });
        }

        await validateAndGetGeoName(sectionId, 'Sections', 'SectionID', 'SectionName');

        const where = ['Year = ?', 'SectionID = ?'];
        const params = [parseInt(year), sectionId];

        const colSelect = columns.join(', ');
        const [rows] = await mysqlPool.query(
            `SELECT ${colSelect} FROM ${table} WHERE ${where.join(' AND ')}`,
            params
        );

        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: `Failed to fetch KPI from ${table}`, details: err.message });
    }
};

// ✅ Unified monthly KPI handler
const fetchMonthlyKPI = (table, valueCol) => async (req, res) => {
    const { sectionId, year } = req.query;

    try {
        if (!sectionId || !year) {
            return res.status(400).json({ error: 'Both sectionId and year are required.' });
        }

        await validateAndGetGeoName(sectionId, 'Sections', 'SectionID', 'SectionName');

        const [rows] = await mysqlPool.query(
            `SELECT Month, ${valueCol} AS value FROM ${table} WHERE Year = ? AND SectionID = ?`,
            [parseInt(year), sectionId]
        );

        const formatted = rows.map(row => ({
            label: row.Month,
            value: row.value
        }));

        res.json(formatted);
    } catch (err) {
        res.status(500).json({ error: `Failed to fetch KPI from ${table}`, details: err.message });
    }
};

// === KPI ROUTES ===

// Year-only KPIs
app.get('/api/kpi/arrear-aging', fetchKPI('ArrearAgingKPI', ['Year', 'TotalOutstanding', 'AgeBucket_0_30', 'AgeBucket_31_60', 'AgeBucket_61_90', 'AgeBucket_90_Plus', 'HighRiskConsumers']));
app.get('/api/kpi/consumption-anomaly', fetchKPI('ConsumptionAnomalyKPI', ['Year', 'AvgMonthlyConsumption', 'ZeroUsageConsumers', 'SpikeCases', 'DropCases']));
app.get('/api/kpi/disconnection-reconnection', fetchKPI('DisconnectionReconnectionKPI', ['Year', 'TotalDisconnections', 'TotalReconnections', 'AvgReconnectionTimeMinutes', 'TotalReconnectionCharges']));
app.get('/api/kpi/dthealth', fetchKPI('DTHealthKPI', ['Year', 'TotalFailures', 'FailureRate', 'AvgRepairCost', 'AvgHealthIndex']));
app.get('/api/kpi/breakdown-restoration', fetchKPI('BreakdownRestorationKPI', ['Year', 'TotalBreakdowns', 'AvgResponseTimeMinutes', 'RestorationWithin4HrsPercent']));
app.get('/api/kpi/connection-progress', fetchKPI('ConnectionProgressKPI', ['Year', 'AvgApprovalTimeDays', 'PendingDisconnections', 'ConnectionCompletionRate']));
app.get('/api/kpi/feeder-outage', fetchKPI('FeederOutageKPI', ['Year', 'SAIFI', 'SAIDI', 'CAIDI', 'FeederID', 'SubstationID']));
app.get('/api/kpi/atc-loss', fetchKPI('ATCLossKPI', ['Year', 'EnergyInput', 'UnitsBilled', 'UnitsPaid', 'ATCLossPercent', 'BillingEfficiency', 'CollectionEfficiency', 'FeederID']));
app.get('/api/kpi/geo-performance', fetchKPI('GeoPerformanceSnapshot', ['Year', 'Latitude', 'Longitude', 'StatusType', 'StatusColor', 'EventType', 'EventCount']));

// ✅ Monthly KPI routes using unified handler
app.get('/api/kpi/arrear-ratio', fetchMonthlyKPI('arrear_ratio', 'Percent'));
app.get('/api/kpi/unbilled-consumers', fetchMonthlyKPI('unbilled_consumers', 'Percent'));
app.get('/api/kpi/billing-efficiency', fetchMonthlyKPI('billing_efficiency', 'Value'));
app.get('/api/kpi/revenue-collection-efficiency', fetchMonthlyKPI('revenue_collection_efficiency', 'Value'));
app.get('/api/kpi/avg-billing-per-consumer', fetchMonthlyKPI('avg_billing_per_consumer', 'Value'));
app.get('/api/kpi/collection-rate', fetchMonthlyKPI('collection_rate', 'Value'));
app.get('/api/kpi/billing-coverage', fetchMonthlyKPI('billing_coverage', 'Value'));

// Server start
app.listen(port, () => {
    console.log(`✅ Server running on http://localhost:${port}`);
});
