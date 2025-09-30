const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());

// --- NUEVO: CHEQUEO DE VARIABLES DE ENTORNO ---
const requiredEnvVars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'];
const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);

if (missingVars.length > 0) {
    // Si faltan variables, registra un error claro y finaliza.
    const errorMessage = `FATAL ERROR: Faltan las siguientes variables de entorno: ${missingVars.join(', ')}`;
    console.error(errorMessage);
    
    // Configura la API para que solo devuelva un error de configuración
    app.get('/api/pois', (req, res) => {
        res.status(500).json({ error: 'Error de configuración del servidor.', details: errorMessage });
    });

} else {
    // --- EL CÓDIGO NORMAL SE EJECUTA SI TODAS LAS VARIABLES ESTÁN PRESENTES ---
    
    const dbConfig = {
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false }
    };

    const pool = new Pool(dbConfig);

    const industryTagsMap = {
        finance: [['amenity', 'bank'], ['amenity', 'atm'], ['office', 'financial']],
        salud: [['amenity', 'hospital'], ['amenity', 'doctors'], ['amenity', 'clinic'], ['amenity', 'pharmacy']],
        automobiles: [['amenity', 'fuel'], ['shop', 'car_rental'], ['shop', 'car'], ['shop', 'car_repair']],
        education: [['amenity', 'university'], ['amenity', 'college'], ['amenity', 'school']],
        retail: [['shop', null]],
        cpg: [['shop', 'supermarket'], ['shop', 'convenience'], ['shop', 'grocery']],
        supermarkets: [['shop', 'supermarket']],
        sports: [['leisure', 'stadium'], ['leisure', 'sports_centre'], ['leisure', 'pitch']],
        entertainment: [['amenity', 'cinema'], ['amenity', 'theatre'], ['amenity', 'nightclub'], ['tourism', 'museum']],
        technology: [['shop', 'electronics'], ['shop', 'computer'], ['office', 'it']]
    };

    app.get('/api/pois', async (req, res) => {
      const { provincia, industria } = req.query;

      if (!provincia || !industria) {
        return res.status(400).json({ error: 'Faltan los parámetros "provincia" e "industria"' });
      }

      const tags = industryTagsMap[industria];
      if (!tags) {
        return res.status(400).json({ error: 'Industria no válida' });
      }

      let queryValues = [provincia];
      let hstoreConditions = tags.map((tag, index) => {
        const key = tag[0];
        const value = tag[1];
        if (value === null) {
          queryValues.push(key);
          return `exist(p.tags, $${queryValues.length})`;
        } else {
          queryValues.push(key);
          queryValues.push(value);
          return `(p.tags -> $${queryValues.length - 1} = $${queryValues.length})`;
        }
      }).join(' OR ');

      const sqlQuery = {
        text: `
          SELECT p.id, p.name, p.tags, ST_AsGeoJSON(p.geom) AS geometry
          FROM pois AS p
          JOIN limites_administrativos AS l ON ST_Intersects(p.geom, l.geom)
          WHERE l.nombre = $1 AND (${hstoreConditions});
        `,
        values: queryValues
      };

      try {
        console.log(`Ejecutando consulta geoespacial para: provincia=${provincia}, industria=${industria}`);
        const result = await pool.query(sqlQuery);
        
        const geojson = {
          type: "FeatureCollection",
          features: result.rows.map(row => ({
            type: "Feature",
            properties: { id: row.id, name: row.name, tags: row.tags },
            geometry: JSON.parse(row.geometry)
          }))
        };
        
        res.status(200).json(geojson);

      } catch (error) {
        console.error('Error en la consulta a la base de datos:', error);
        res.status(500).json({ error: 'Error interno del servidor' });
      }
    });
}

module.exports = app;