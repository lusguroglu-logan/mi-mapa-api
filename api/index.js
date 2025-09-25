// Importar las librerías necesarias
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

// Crear la aplicación del servidor
const app = express();
app.use(cors()); // Habilitar CORS

// --- CONFIGURACIÓN DE LA BASE DE DATOS (lee desde las Variables de Entorno de Vercel) ---
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false }
};

const pool = new Pool(dbConfig);

// --- MAPEO DE INDUSTRIAS ---
// Mapeo de categorías amigables a consultas de tags en formato HSTORE
const industryTags = {
    gastronomia: `(tags @> '"amenity"=>"restaurant"' OR tags @> '"amenity"=>"cafe"' OR tags @> '"amenity"=>"bar"')`,
    salud: `(tags @> '"amenity"=>"pharmacy"' OR tags @> '"amenity"=>"doctors"')`,
    automotor: `(tags @> '"shop"="car_repair"' OR tags @> '"amenity"="fuel"')`
    // Puedes añadir más industrias aquí
};

// --- RUTA PRINCIPAL PARA BUSCAR PUNTOS DE INTERÉS (POIs) ---
app.get('/api/pois', async (req, res) => {
  const { provincia, industria } = req.query;

  if (!provincia || !industria) {
    return res.status(400).json({ error: 'Faltan los parámetros "provincia" e "industria"' });
  }

  const tagQuery = industryTags[industria];
  if (!tagQuery) {
    return res.status(400).json({ error: 'Industria no válida' });
  }

  // Consulta SQL Geoespacial
  // Une 'pois' y 'limites_administrativos' y usa ST_Within para filtrar geográficamente
  const sqlQuery = {
    text: `
      SELECT p.id, p.name, p.tags, ST_AsGeoJSON(p.geom) AS geometry
      FROM pois AS p
      JOIN limites_administrativos AS l ON ST_Within(p.geom, l.geom)
      WHERE l.nombre = $1 AND ${tagQuery};
    `,
    values: [provincia]
  };

  try {
    console.log(`Ejecutando consulta geoespacial para: provincia=${provincia}, industria=${industria}`);
    const result = await pool.query(sqlQuery);
    
    // Convertir el resultado a formato GeoJSON, que Leaflet entiende
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

// --- RUTA OPCIONAL PARA OBTENER LOS LÍMITES DE UNA PROVINCIA ---
app.get('/api/limites', async (req, res) => {
    const { provincia } = req.query;
    if (!provincia) {
        return res.status(400).json({ error: 'Falta el parámetro "provincia"' });
    }
    const sqlQuery = {
        text: `SELECT nombre, ST_AsGeoJSON(geom) as geometry FROM limites_administrativos WHERE nombre = $1;`,
        values: [provincia]
    };
    try {
        const result = await pool.query(sqlQuery);
        res.status(200).json(result.rows.map(row => ({
            name: row.nombre,
            geometry: JSON.parse(row.geometry)
        })));
    } catch (error) {
        console.error('Error en la consulta de límites:', error);
        res.status(500).json({ error: 'Error interno del servidor' });
    }
});


// Exportar la app para que Vercel la pueda usar
module.exports = app;