-- Script de inicialización de la base de datos de calidad del agua
-- Se ejecuta automáticamente cuando se crea el contenedor

-- Crear esquema para los datos
CREATE SCHEMA IF NOT EXISTS water_data;

-- Eliminar tabla y vistas si existen (para re-inicialización limpia)
DROP VIEW IF EXISTS water_data.calidad_por_periodo CASCADE;
DROP VIEW IF EXISTS water_data.resumen_por_estado CASCADE;
DROP TABLE IF EXISTS water_data.calidad_agua_clean CASCADE;

-- Crear tabla para datos limpios de calidad del agua
CREATE TABLE IF NOT EXISTS water_data.calidad_agua_clean (
    id SERIAL PRIMARY KEY,
    lave_sitio VARCHAR(50),
    sitio VARCHAR(255),
    organismo_de_cuenca VARCHAR(255),
    estado VARCHAR(100),
    municipio VARCHAR(100),
    cuenca VARCHAR(255),
    cuerpo_de_agua VARCHAR(255),
    tipo VARCHAR(50),
    subtipo VARCHAR(50),
    longitud DECIMAL(10, 6),
    latitud DECIMAL(10, 6),
    periodo VARCHAR(50),
    dbo_mg_l DECIMAL(10, 2),
    calidad_dbo VARCHAR(50),
    dqo_mg_l DECIMAL(10, 2),
    calidad_dqo VARCHAR(50),
    sst_mg_l DECIMAL(10, 2),
    calidad_sst VARCHAR(50),
    semaforo VARCHAR(20),
    contaminantes TEXT,
    -- Columnas agregadas por el ETL
    indice_calidad_dqo INTEGER,
    indice_calidad_dbo INTEGER,
    indice_calidad_sst INTEGER,
    indice_calidad_general DECIMAL(5, 2),
    semaforo_numerico INTEGER,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear índices para mejorar consultas
CREATE INDEX IF NOT EXISTS idx_estado ON water_data.calidad_agua_clean(estado);
CREATE INDEX IF NOT EXISTS idx_semaforo ON water_data.calidad_agua_clean(semaforo);
CREATE INDEX IF NOT EXISTS idx_organismo ON water_data.calidad_agua_clean(organismo_de_cuenca);
CREATE INDEX IF NOT EXISTS idx_fecha_carga ON water_data.calidad_agua_clean(fecha_carga);

-- Crear vista para análisis rápido
CREATE OR REPLACE VIEW water_data.resumen_por_estado AS
SELECT 
    estado,
    COUNT(*) as total_sitios,
    AVG(indice_calidad_general) as calidad_promedio,
    COUNT(CASE WHEN semaforo = 'VERDE' THEN 1 END) as sitios_verdes,
    COUNT(CASE WHEN semaforo = 'AMARILLO' THEN 1 END) as sitios_amarillos,
    COUNT(CASE WHEN semaforo = 'ROJO' THEN 1 END) as sitios_rojos
FROM water_data.calidad_agua_clean
GROUP BY estado
ORDER BY calidad_promedio DESC;

-- Crear vista para análisis temporal
CREATE OR REPLACE VIEW water_data.calidad_por_periodo AS
SELECT 
    periodo,
    estado,
    COUNT(*) as mediciones,
    AVG(indice_calidad_general) as calidad_promedio,
    MIN(indice_calidad_general) as calidad_minima,
    MAX(indice_calidad_general) as calidad_maxima
FROM water_data.calidad_agua_clean
GROUP BY periodo, estado
ORDER BY periodo DESC, estado;

-- Otorgar permisos
GRANT ALL PRIVILEGES ON SCHEMA water_data TO wateruser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA water_data TO wateruser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA water_data TO wateruser;

-- Log de inicialización
INSERT INTO water_data.calidad_agua_clean 
    (lave_sitio, sitio, estado, created_at) 
VALUES 
    ('INIT', 'Sistema inicializado', 'SISTEMA', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

-- Mensaje de confirmación
DO $$
BEGIN
    RAISE NOTICE 'Base de datos water_quality_db inicializada correctamente';
    RAISE NOTICE 'Esquema: water_data';
    RAISE NOTICE 'Tabla principal: water_data.calidad_agua_clean';
END $$;