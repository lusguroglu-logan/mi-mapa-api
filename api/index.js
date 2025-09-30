// api/index.js - VERSIÓN FINAL

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());

// Configuración de la base de datos (lee desde las Variables de Entorno)
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false }
};

const pool = new Pool(dbConfig);

// --- MAPEO DE INDUSTRIAS CORREGIDO ---
// Ahora busca el texto de cada tag dentro del campo 'other_tags'
const industryTags = {
    finance: `((tags -> 'other_tags') LIKE '%"amenity"=>"bank"%')`,
    salud: `((tags -> 'other_tags') LIKE '%"amenity"=>"hospital"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"doctors"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"clinic"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"pharmacy"%')`,
    automobiles: `((tags -> 'other_tags') LIKE '%"amenity"=>"fuel"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"car_rental"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"car"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"car_repair"%')`,
    education: `((tags -> 'other_tags') LIKE '%"amenity"=>"university"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"college"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"school"%')`,
    retail: `((tags -> 'other_tags') LIKE '%"shop"=>%')`,
    cpg: `((tags -> 'other_tags') LIKE '%"shop"=>"supermarket"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"convenience"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"grocery"%')`,
    supermarkets: `((tags -> 'other_tags') LIKE '%"shop"=>"supermarket"%')`,
    sports: `((tags -> 'other_tags') LIKE '%"leisure"=>"stadium"%' OR (tags -> 'other_tags') LIKE '%"leisure"=>"sports_centre"%' OR (tags -> 'other_tags') LIKE '%"leisure"=>"pitch"%')`,
    entertainment: `((tags -> 'other_tags') LIKE '%"amenity"=>"cinema"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"theatre"%' OR (tags -> 'other_tags') LIKE '%"amenity"=>"nightclub"%' OR (tags -> 'other_tags') LIKE '%"tourism"=>"museum"%')`,
    technology: `((tags -> 'other_tags') LIKE '%"shop"=>"electronics"%' OR (tags -> 'other_tags') LIKE '%"shop"=>"computer"%' OR (tags -> 'other_tags') LIKE '%"office"=>"it"%')`
};

app.get('/api/pois', async (req, res) => {
  const { provincia, industria } = req.query;

  if (!provincia || !industria) {
    return res.status(400).json({ error: 'Faltan los parámetros "provincia" e "industria"' });
  }

  const tagQuery = industryTags[industria];
  if (!tagQuery) {
    return res.status(400).json({ error: 'Industria no válida' });
  }

  // --- CONSULTA SQL FINAL ---
  const sqlQuery = {
    text: `
      SELECT p.id, p.name, p.tags, ST_AsGeoJSON(p.geom) AS geometry
      FROM pois AS p
      JOIN limites_administrativos AS l ON ST_Intersects(p.geom, l.geom)
      WHERE l.nombre = $1 AND ${tagQuery};
    `,
    values: [provincia]
  };

  try {
    console.log(`Ejecutando consulta final para: provincia=${provincia}, industria=${industria}`);
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

module.exports = app;