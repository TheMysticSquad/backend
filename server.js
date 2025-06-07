require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise'); // Using mysql2/promise for async/await

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// MySQL Pool for all data (KPIs and geographical master data)
const parseMysqlUrl = (mysqlUrl) => {
    const parsed = new URL(mysqlUrl);
    return {
        host: parsed.hostname,
        port: parsed.port,
        user: parsed.username,
        password: parsed.password,
        database: parsed.pathname.replace('/', ''),
        ssl: { rejectUnauthorized: false } // Adjust as per your MySQL server's SSL config
    };
};

const mysqlPool = mysql.createPool(parseMysqlUrl(process.env.KPI_DB_URL));

// Middleware for basic logging and CORS
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.originalUrl}`);
    next();
});

// --- Helper for ID validation and existence checking ---
const validateAndGetGeoName = async (id, tableName, idCol, nameCol, parentId, parentIdCol) => {
    if (id === undefined) return null; // Not provided, not an error

    // 1. Validate if it's an integer
    if (isNaN(parseInt(id)) || String(parseInt(id)) !== String(id)) {
        throw new Error(`Invalid ID format for ${idCol}: ${id}. Must be an integer.`);
    }
    const parsedId = parseInt(id);

    let query = `SELECT ${nameCol} FROM ${tableName} WHERE ${idCol} = ?`;
    const params = [parsedId];

    // 2. Validate hierarchy if parentId is provided
    if (parentId !== undefined && parentId !== null) {
        if (isNaN(parseInt(parentId)) || String(parseInt(parentId)) !== String(parentId)) {
            throw new Error(`Invalid parent ID format for ${parentIdCol}: ${parentId}. Must be an integer.`);
        }
        query += ` AND ${parentIdCol} = ?`;
        params.push(parseInt(parentId));
    }

    const [rows] = await mysqlPool.query(query, params);

    // 3. Check if ID exists and belongs to hierarchy
    if (rows.length === 0) {
        if (parentId !== undefined && parentId !== null) {
            throw new Error(`Invalid ${idCol}: ${id} does not exist or does not belong to ${parentIdCol}: ${parentId}.`);
        } else {
            throw new Error(`Invalid ${idCol}: ${id} does not exist.`);
        }
    }
    return rows[0][nameCol];
};


/* ---------- Master Data Endpoints (MySQL) ---------- */

app.get('/api/filters/circles', async (req, res) => {
    try {
        const [rows] = await mysqlPool.query(`SELECT CircleID AS id, CircleName AS name FROM Circles ORDER BY CircleName;`);
        res.json(rows);
    } catch (err) {
        console.error('Error fetching circles:', err);
        res.status(500).json({ error: 'Failed to fetch circles' });
    }
});

app.get('/api/filters/divisions', async (req, res) => {
    const { circleId } = req.query;
    try {
        const circleName = await validateAndGetGeoName(circleId, 'Circles', 'CircleID', 'CircleName'); // Check circleId exists
        if (!circleName) { // Means circleId was missing or invalid
            return res.status(400).json({ error: 'circleId is required and must be valid' });
        }
        
        const [rows] = await mysqlPool.query(
            `SELECT DivisionID AS id, DivisionName AS name, CircleID FROM Divisions WHERE CircleID = ? ORDER BY DivisionName;`,
            [circleId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching divisions:', err);
        if (err.message.startsWith('Invalid ID format') || err.message.startsWith('Invalid CircleID')) {
             return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Failed to fetch divisions' });
    }
});

app.get('/api/filters/subdivisions', async (req, res) => {
    const { divisionId } = req.query;
    try {
        const divisionName = await validateAndGetGeoName(divisionId, 'Divisions', 'DivisionID', 'DivisionName'); // Check divisionId exists
        if (!divisionName) { // Means divisionId was missing or invalid
            return res.status(400).json({ error: 'divisionId is required and must be valid' });
        }

        const [rows] = await mysqlPool.query(
            `SELECT SubdivisionID AS id, SubdivisionName AS name, DivisionID FROM Subdivisions WHERE DivisionID = ? ORDER BY SubdivisionName;`,
            [divisionId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching subdivisions:', err);
        if (err.message.startsWith('Invalid ID format') || err.message.startsWith('Invalid DivisionID')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Failed to fetch subdivisions' });
    }
});

app.get('/api/filters/sections', async (req, res) => {
    const { subdivisionId } = req.query;
    try {
        const subdivisionName = await validateAndGetGeoName(subdivisionId, 'Subdivisions', 'SubdivisionID', 'SubdivisionName'); // Check subdivisionId exists
        if (!subdivisionName) { // Means subdivisionId was missing or invalid
            return res.status(400).json({ error: 'subdivisionId is required and must be valid' });
        }
        
        const [rows] = await mysqlPool.query(
            `SELECT SectionID AS id, SectionName AS name, SubdivisionID FROM Sections WHERE SubdivisionID = ? ORDER BY SectionName;`,
            [subdivisionId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching sections:', err);
        if (err.message.startsWith('Invalid ID format') || err.message.startsWith('Invalid SubdivisionID')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Failed to fetch sections' });
    }
});

app.get('/api/filters/years', async (req, res) => {
    try {
        const [rows] = await mysqlPool.query(`
            SELECT DISTINCT year AS year_val
            FROM billing_efficiency
            ORDER BY year_val DESC;
        `);
        const years = rows.map(row => String(row.year_val)); // Ensure years are strings
        res.json(years);
    } catch (err) {
        console.error('Error fetching years:', err);
        res.status(500).json({ error: 'Failed to fetch years' });
    }
});

/* ---------- KPI Endpoints (MySQL with direct filtering) ---------- */

const fetchKPI = (tableName, labelKey, valueKey) => async (req, res) => {
    const { circleId, divisionId, subdivisionId, sectionId, year } = req.query;

    try {
        // --- 1. Validate REQUIRED parameters ---
        if (!circleId) {
            return res.status(400).json({ error: 'circleId is required for KPI data.' });
        }
        if (!year) {
            return res.status(400).json({ error: 'year is required for KPI data.' });
        }
        if (isNaN(parseInt(year)) || String(parseInt(year)) !== String(year)) {
            return res.status(400).json({ error: `Invalid year format: ${year}. Must be an integer.` });
        }

        // --- 2. Validate IDs and Hierarchy, get names for WHERE clauses ---
        let circleName, divisionName, subdivisionName, sectionName;

        circleName = await validateAndGetGeoName(circleId, 'Circles', 'CircleID', 'CircleName');
        // If circleName is null, validateAndGetGeoName would have thrown.

        if (divisionId) {
            divisionName = await validateAndGetGeoName(divisionId, 'Divisions', 'DivisionID', 'DivisionName', circleId, 'CircleID');
        }
        if (subdivisionId) {
            subdivisionName = await validateAndGetGeoName(subdivisionId, 'Subdivisions', 'SubdivisionID', 'SubdivisionName', divisionId, 'DivisionID');
        }
        if (sectionId) {
            sectionName = await validateAndGetGeoName(sectionId, 'Sections', 'SectionID', 'SectionName', subdivisionId, 'SubdivisionID');
        }

        const queryParams = [];
        const whereClauses = [];

        // Always add circle and year as they are required and validated above
        whereClauses.push(`circle = ?`);
        queryParams.push(circleName);
        whereClauses.push(`year = ?`);
        queryParams.push(parseInt(year));

        if (divisionName) {
            whereClauses.push(`division = ?`);
            queryParams.push(divisionName);
        }
        if (subdivisionName) {
            whereClauses.push(`subdivision = ?`);
            queryParams.push(subdivisionName);
        }
        if (sectionName) {
            whereClauses.push(`section = ?`);
            queryParams.push(sectionName);
        }

        let query = `
            SELECT
                ${labelKey} AS month_or_section_label, 
                year,
                ${valueKey} AS value,
                -- Dynamic CONCAT based on labelKey
                CASE
                    WHEN '${labelKey}' = 'section' THEN ${labelKey} 
                    ELSE CONCAT(${labelKey}, ' ', year) 
                END AS label 
            FROM ${tableName}
        `;

        if (whereClauses.length > 0) {
            query += ` WHERE ${whereClauses.join(' AND ')}`;
        }

        query += ` ORDER BY year, month_or_section_label;`; 

        const [rows] = await mysqlPool.query(query, queryParams);
        const formatted = rows.map(row => ({
            label: row.label,
            value: row.value
        }));
        res.json(formatted);

    } catch (err) {
        console.error(`Error fetching KPI ${tableName}:`, err);
        // Distinguish between validation errors (400) and internal server errors (500)
        if (err.message.startsWith('Invalid ID format') || err.message.startsWith('Invalid CircleID') ||
            err.message.startsWith('Invalid DivisionID') || err.message.startsWith('Invalid SubdivisionID') ||
            err.message.startsWith('Invalid SectionID') || err.message.includes('required')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: `Failed to fetch KPI: ${tableName}` });
    }
};

// All existing KPI endpoints will now use the modified fetchKPI
app.get('/api/kpi/billing-efficiency', fetchKPI('billing_efficiency', 'month', 'value'));
app.get('/api/kpi/revenue-collection-efficiency', fetchKPI('revenue_collection_efficiency', 'month', 'value'));
app.get('/api/kpi/avg-billing-per-consumer', fetchKPI('avg_billing_per_consumer', 'month', 'value'));
app.get('/api/kpi/billing-rate', fetchKPI('billing_rate', 'section', 'rate')); // labelKey is 'section', valueKey is 'rate'
app.get('/api/kpi/collection-rate', fetchKPI('collection_rate', 'month', 'value'));
app.get('/api/kpi/unbilled-consumers', fetchKPI('unbilled_consumers', 'month', 'percent'));
app.get('/api/kpi/arrear-ratio', fetchKPI('arrear_ratio', 'month', 'percent'));
app.get('/api/kpi/billing-coverage', fetchKPI('billing_coverage', 'month', 'value'));

// Custom endpoints for special tables (disputed_bills, metering_status)
// These do NOT have geographical filters. Only year filter applies.
app.get('/api/kpi/disputed-bills', async (req, res) => {
    const { year: filterYear } = req.query;

    try {
        if (!filterYear) {
            return res.status(400).json({ error: 'year is required for disputed-bills KPI.' });
        }
        if (isNaN(parseInt(filterYear)) || String(parseInt(filterYear)) !== String(filterYear)) {
            return res.status(400).json({ error: `Invalid year format: ${filterYear}. Must be an integer.` });
        }

        let query = `SELECT month, year, total, disputed FROM disputed_bills WHERE year = ? ORDER BY year, month;`;
        const [rows] = await mysqlPool.query(query, [parseInt(filterYear)]);
        
        // Convert to label/value format
        const formatted = rows.map(row => ({
            label: `${row.month} ${row.year}`, // Example: "January 2024" or "1 2024"
            value: row.total // Or whatever primary value you want for this KPI's main chart
        }));
        res.json(formatted);
    } catch (err) {
        console.error('Error fetching KPI disputed_bills:', err);
        if (err.message.startsWith('Invalid year format')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Failed to fetch KPI: disputed_bills' });
    }
});

app.get('/api/kpi/metering-status', async (req, res) => {
    const { year: filterYear } = req.query;

    try {
        if (!filterYear) {
            return res.status(400).json({ error: 'year is required for metering-status KPI.' });
        }
        if (isNaN(parseInt(filterYear)) || String(parseInt(filterYear)) !== String(filterYear)) {
            return res.status(400).json({ error: `Invalid year format: ${filterYear}. Must be an integer.` });
        }

        let query = `SELECT month, year, metered, unmetered FROM metering_status WHERE year = ? ORDER BY year, month;`;
        const [rows] = await mysqlPool.query(query, [parseInt(filterYear)]);
        
        // Convert to label/value format
        const formatted = rows.map(row => ({
            label: `${row.month} ${row.year}`, // Example: "January 2024" or "1 2024"
            value: row.metered // Or whatever primary value you want for this KPI's main chart
        }));
        res.json(formatted);
    } catch (err) {
        console.error('Error fetching KPI metering_status:', err);
        if (err.message.startsWith('Invalid year format')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: 'Failed to fetch KPI: metering_status' });
    }
});


/* ---------- Start Server ---------- */
app.listen(port, () => {
    console.log(`✅ Server running on http://localhost:${port}`);
});
