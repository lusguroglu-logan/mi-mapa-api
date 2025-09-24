// Importar las librerías necesarias
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

// Crear la aplicación del servidor
const app = express();
app.use(cors()); // Habilitar CORS para que el mapa pueda hacer peticiones

// --- CONFIGURACIÓN DE LA BASE DE DATOS ---
// Usa las credenciales del CONNECTION POOLER de Supabase
const dbConfig = {
  host: "aws-1-sa-east-1.pooler.supabase.com",
  port: 6543,
  database: "postgres",
  user: "postgres.pvfkmhikrhxdciuypxru",
  password: "j2OQpNfA4CNK1z3f",
  ssl: { rejectUnauthorized: false } // Necesario para conexiones a Supabase
};

const pool = new Pool(dbConfig);

// --- DEFINICIÓN DE LA RUTA DE LA API ---
// Esta será la URL que tu mapa consultará
app.get('/api/pois', async (req, res) => {
  // Obtener los parámetros de la consulta (ej: ?provincia=la-rioja&industria=gastronomia)
  const { provincia, industria } = req.query;

  if (!provincia || !industria) {
    return res.status(400).json({ error: 'Faltan los parámetros "provincia" e "industria"' });
  }

  // Mapeo de industrias a la consulta de tags en HSTORE
  // (Este es un ejemplo, puedes expandirlo)
  const industryTags = {
    gastronomia: `(tags @> '"amenity"=>"restaurant"' OR tags @> '"amenity"=>"cafe"')`,
    salud: `(tags @> '"amenity"=>"pharmacy"' OR tags @> '"amenity"=>"doctors"')`
    // Añade más industrias aquí
  };

  const tagQuery = industryTags[industria];
  if (!tagQuery) {
    return res.status(400).json({ error: 'Industria no válida' });
  }

  // Consulta SQL a PostGIS
  // Buscamos puntos donde la etiqueta "is_in:province" coincida
  // y que cumplan con la condición de la industria.
  const sqlQuery = `
    SELECT id, name, tags, ST_AsGeoJSON(geom) AS geometry
    FROM pois
    WHERE tags @> '"is_in:province"=>"${provincia}"' AND ${tagQuery};
  `;

  try {
    console.log(`Ejecutando consulta para: provincia=${provincia}, industria=${industria}`);
    const result = await pool.query(sqlQuery);
    
    // Convertir el resultado a formato GeoJSON, que Leaflet entiende perfectamente
    const geojson = {
      type: "FeatureCollection",
      features: result.rows.map(row => ({
        type: "Feature",
        properties: {
          id: row.id,
          name: row.name,
          tags: row.tags
        },
        geometry: JSON.parse(row.geometry)
      }))
    };
    
    res.status(200).json(geojson);

  } catch (error) {
    console.error('Error en la consulta a la base de datos:', error);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// Exportar la app para que Vercel la pueda usar
module.exports = app;