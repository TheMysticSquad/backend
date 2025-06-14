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

app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.originalUrl}`);
    next();
});

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

// Filter endpoints
app.get('/api/filters/circles', async (req, res) => {
    try {
        const [rows] = await mysqlPool.query(`SELECT CircleID AS id, CircleName AS name FROM Circles ORDER BY CircleName;`);
        res.json(rows);
    } catch (err) {
        console.error('Error fetching circles:', err);
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
        console.error('Error fetching divisions:', err);
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
        console.error('Error fetching subdivisions:', err);
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
        console.error('Error fetching sections:', err);
        res.status(400).json({ error: err.message });
    }
});

app.get('/api/filters/years', async (req, res) => {
    try {
        const [rows] = await mysqlPool.query(`SELECT DISTINCT Year FROM BillingCollectionSummary ORDER BY Year DESC;`);
        const years = rows.map(r => String(r.Year));
        res.json(years);
    } catch (err) {
        console.error('Error fetching years:', err);
        res.status(500).json({ error: 'Failed to fetch years' });
    }
});

const fetchKPI = (table, columns) => async (req, res) => {
    const { circleId, divisionId, subdivisionId, sectionId, year } = req.query;
    try {
        if (!circleId || !year) return res.status(400).json({ error: 'circleId and year are required.' });
        const filters = { CircleID: circleId, DivisionID: divisionId, SubdivisionID: subdivisionId, SectionID: sectionId };
        const where = ['Year = ?'];
        const params = [parseInt(year)];

        for (const key of ['CircleID', 'DivisionID', 'SubdivisionID', 'SectionID']) {
            if (filters[key]) {
                await validateAndGetGeoName(filters[key], key.replace('ID', 's'), key, key.replace('ID', 'Name'));
                where.push(`${key} = ?`);
                params.push(filters[key]);
            }
        }

        const colSelect = columns.join(', ');
        const [rows] = await mysqlPool.query(`SELECT ${colSelect} FROM ${table} WHERE ${where.join(' AND ')}`, params);
        res.json(rows);
    } catch (err) {
        console.error(`Error fetching KPI ${table}:`, err);
        res.status(400).json({ error: err.message });
    }
};

// Combined endpoints for all KPIs
app.get('/api/kpi/arrear-aging', fetchKPI('ArrearAgingKPI', ['Year', 'TotalOutstanding', 'AgeBucket_0_30', 'AgeBucket_31_60', 'AgeBucket_61_90', 'AgeBucket_90_Plus', 'HighRiskConsumers']));
app.get('/api/kpi/consumption-anomaly', fetchKPI('ConsumptionAnomalyKPI', ['Year', 'AvgMonthlyConsumption', 'ZeroUsageConsumers', 'SpikeCases', 'DropCases']));
app.get('/api/kpi/disconnection-reconnection', fetchKPI('DisconnectionReconnectionKPI', ['Year', 'TotalDisconnections', 'TotalReconnections', 'AvgReconnectionTimeMinutes', 'TotalReconnectionCharges']));
app.get('/api/kpi/dthealth', fetchKPI('DTHealthKPI', ['Year', 'TotalFailures', 'FailureRate', 'AvgRepairCost', 'AvgHealthIndex']));
app.get('/api/kpi/breakdown-restoration', fetchKPI('BreakdownRestorationKPI', ['Year', 'TotalBreakdowns', 'AvgResponseTimeMinutes', 'RestorationWithin4HrsPercent']));
app.get('/api/kpi/connection-progress', fetchKPI('ConnectionProgressKPI', ['Year', 'AvgApprovalTimeDays', 'PendingDisconnections', 'ConnectionCompletionRate']));
app.get('/api/kpi/feeder-outage', fetchKPI('FeederOutageKPI', ['Year', 'SAIFI', 'SAIDI', 'CAIDI', 'FeederID', 'SubstationID']));
app.get('/api/kpi/atc-loss', fetchKPI('ATCLossKPI', ['Year', 'EnergyInput', 'UnitsBilled', 'UnitsPaid', 'ATCLossPercent', 'BillingEfficiency', 'CollectionEfficiency', 'FeederID']));
app.get('/api/kpi/geo-performance', fetchKPI('GeoPerformanceSnapshot', ['Year', 'Latitude', 'Longitude', 'StatusType', 'StatusColor', 'EventType', 'EventCount']));

// Add previous KPI endpoints
app.get('/api/kpi/billing-efficiency', fetchKPI('billing_efficiency', ['Month', 'Year', 'Value']));
app.get('/api/kpi/revenue-collection-efficiency', fetchKPI('revenue_collection_efficiency', ['Month', 'Year', 'Value']));
app.get('/api/kpi/avg-billing-per-consumer', fetchKPI('avg_billing_per_consumer', ['Month', 'Year', 'Value']));
app.get('/api/kpi/billing-rate', fetchKPI('billing_rate', ['Section', 'Year', 'Rate']));
app.get('/api/kpi/collection-rate', fetchKPI('collection_rate', ['Month', 'Year', 'Value']));
app.get('/api/kpi/unbilled-consumers', fetchKPI('unbilled_consumers', ['Month', 'Year', 'Percent']));
app.get('/api/kpi/arrear-ratio', fetchKPI('arrear_ratio', ['Month', 'Year', 'Percent']));
app.get('/api/kpi/billing-coverage', fetchKPI('billing_coverage', ['Month', 'Year', 'Value']));

app.listen(port, () => {
    console.log(`✅ Server running on http://localhost:${port}`);
});
