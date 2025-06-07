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

/* ---------- Master Data Endpoints (MySQL) ---------- */
// These remain the same as the geographical tables (Circles, Divisions, etc.)
// are distinct from the KPI tables, and this hierarchy is needed for dropdowns.

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
    if (!circleId) return res.status(400).json({ error: 'circleId is required' });

    try {
        const [rows] = await mysqlPool.query(
            `SELECT DivisionID AS id, DivisionName AS name, CircleID FROM Divisions WHERE CircleID = ? ORDER BY DivisionName;`,
            [circleId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching divisions:', err);
        res.status(500).json({ error: 'Failed to fetch divisions' });
    }
});

app.get('/api/filters/subdivisions', async (req, res) => {
    const { divisionId } = req.query;
    if (!divisionId) return res.status(400).json({ error: 'divisionId is required' });

    try {
        const [rows] = await mysqlPool.query(
            `SELECT SubdivisionID AS id, SubdivisionName AS name, DivisionID FROM Subdivisions WHERE DivisionID = ? ORDER BY SubdivisionName;`,
            [divisionId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching subdivisions:', err);
        res.status(500).json({ error: 'Failed to fetch subdivisions' });
    }
});

app.get('/api/filters/sections', async (req, res) => {
    const { subdivisionId } = req.query;
    if (!subdivisionId) return res.status(400).json({ error: 'subdivisionId is required' });

    try {
        // Assuming we prioritize subdivision for sections.
        // If a section can exist without a subdivision but linked to a division,
        // you might need additional logic here (e.g., if (!subdivisionId && divisionId) ...)
        const [rows] = await mysqlPool.query(
            `SELECT SectionID AS id, SectionName AS name, SubdivisionID FROM Sections WHERE SubdivisionID = ? ORDER BY SectionName;`,
            [subdivisionId]
        );
        res.json(rows);
    } catch (err) {
        console.error('Error fetching sections:', err);
        res.status(500).json({ error: 'Failed to fetch sections' });
    }
});

app.get('/api/filters/years', async (req, res) => {
    try {
        // Fetch distinct years from one of your KPI tables (e.g., billing_efficiency)
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

// ... (previous code for imports, app setup, mysqlPool) ...

/* ---------- Master Data Endpoints (MySQL) ---------- */
// ... (Your /api/filters/circles, divisions, subdivisions, sections, years endpoints - these remain unchanged) ...

/* ---------- KPI Endpoints (MySQL with direct filtering) ---------- */

// Helper function to lookup names from IDs
const getGeoNamesFromIds = async ({ circleId, divisionId, subdivisionId, sectionId }) => {
    const geoNames = {};
    try {
        if (circleId) {
            const [rows] = await mysqlPool.query(`SELECT CircleName FROM Circles WHERE CircleID = ?`, [circleId]);
            if (rows.length > 0) geoNames.circleName = rows[0].CircleName;
        }
        if (divisionId) {
            const [rows] = await mysqlPool.query(`SELECT DivisionName FROM Divisions WHERE DivisionID = ?`, [divisionId]);
            if (rows.length > 0) geoNames.divisionName = rows[0].DivisionName;
        }
        if (subdivisionId) {
            const [rows] = await mysqlPool.query(`SELECT SubdivisionName FROM Subdivisions WHERE SubdivisionID = ?`, [subdivisionId]);
            if (rows.length > 0) geoNames.subdivisionName = rows[0].SubdivisionName;
        }
        if (sectionId) {
            const [rows] = await mysqlPool.query(`SELECT SectionName FROM Sections WHERE SectionID = ?`, [sectionId]);
            if (rows.length > 0) geoNames.sectionName = rows[0].SectionName;
        }
    } catch (error) {
        console.error("Error looking up geographical names:", error);
        // Depending on strictness, you might want to throw or return nulls
        // For now, it will just proceed with whatever names it found
    }
    return geoNames;
};


const fetchKPI = (tableName, labelKey, valueKey) => async (req, res) => {
    // Extract incoming IDs and year
    const { circleId, divisionId, subdivisionId, sectionId, year } = req.query;

    // Perform ID to Name lookup
    const { circleName, divisionName, subdivisionName, sectionName } = await getGeoNamesFromIds({
        circleId, divisionId, subdivisionId, sectionId
    });

    const queryParams = [];
    const whereClauses = [];

    // Apply filters using the looked-up names (or original IDs if lookup failed/not provided)
    if (circleName) {
        whereClauses.push(`circle = ?`);
        queryParams.push(circleName);
    }
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
    if (year) { // Year is directly applicable as INT
        whereClauses.push(`year = ?`);
        queryParams.push(year);
    }

    let query = `
        SELECT
            ${labelKey} AS month_label, -- Renaming to avoid conflict with 'label' alias
            year,
            ${valueKey} AS value,
            CONCAT(${labelKey}, ' ', year) AS label -- Concatenate for frontend label
        FROM ${tableName}
    `;

    if (whereClauses.length > 0) {
        query += ` WHERE ${whereClauses.join(' AND ')}`;
    }

    query += ` ORDER BY year, ${labelKey};`; // Assuming month-like labels can be ordered lexicographically with year for proper sorting

    try {
        const [rows] = await mysqlPool.query(query, queryParams);
        const formatted = rows.map(row => ({
            label: row.label, // Use the concatenated label
            value: row.value
        }));
        res.json(formatted); // REMOVED: { data: ... } wrapper
    } catch (err) {
        console.error(`Error fetching KPI ${tableName}:`, err);
        res.status(500).json({ error: `Failed to fetch KPI: ${tableName}` });
    }
};

// All existing KPI endpoints will now use the modified fetchKPI
// labelKey for month-based KPIs should be 'month'
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
    const { year: filterYear } = req.query; // Only year filter applies

    let query = `SELECT month, year, total, disputed FROM disputed_bills`;
    const queryParams = [];
    const whereClauses = [];

    if (filterYear) {
        whereClauses.push(`year = ?`);
        queryParams.push(filterYear);
    }

    if (whereClauses.length > 0) {
        query += ` WHERE ${whereClauses.join(' AND ')}`;
    }
    query += ` ORDER BY year, month;`; // Order by year and month

    try {
        const [rows] = await mysqlPool.query(query, queryParams);
        res.json(rows); // REMOVED: { data: ... } wrapper
    } catch (err) {
        console.error('Error fetching KPI disputed_bills:', err);
        res.status(500).json({ error: 'Failed to fetch KPI: disputed_bills' });
    }
});

app.get('/api/kpi/metering-status', async (req, res) => {
    const { year: filterYear } = req.query; // Only year filter applies

    let query = `SELECT month, year, metered, unmetered FROM metering_status`;
    const queryParams = [];
    const whereClauses = [];

    if (filterYear) {
        whereClauses.push(`year = ?`);
        queryParams.push(filterYear);
    }

    if (whereClauses.length > 0) {
        query += ` WHERE ${whereClauses.join(' AND ')}`;
    }
    query += ` ORDER BY year, month;`; // Order by year and month

    try {
        const [rows] = await mysqlPool.query(query, queryParams);
        res.json(rows); // REMOVED: { data: ... } wrapper
    } catch (err) {
        console.error('Error fetching KPI metering_status:', err);
        res.status(500).json({ error: 'Failed to fetch KPI: metering_status' });
    }
});


/* ---------- Start Server ---------- */
app.listen(port, () => {
    console.log(`✅ Server running on http://localhost:${port}`);
});