from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.transform import transform_quality_data
import os
import logging

logger = logging.getLogger(__name__)

# Rutas
BASE_DIR = "/opt/airflow/data"
RAW_FILE = os.path.join(BASE_DIR, "calidad_agua.csv")
TRANSFORMED_FILE_CSV = os.path.join(BASE_DIR, "calidad_agua_clean.csv")
TRANSFORMED_FILE_PARQUET = os.path.join(BASE_DIR, "calidad_agua_clean.parquet")

# Configuración de la base de datos
WATER_DB_CONFIG = {
    'host': os.getenv('WATER_DB_HOST', 'postgres-waterdb'),
    'port': os.getenv('WATER_DB_PORT', '5432'),
    'database': os.getenv('WATER_DB_NAME', 'water_quality_db'),
    'user': os.getenv('WATER_DB_USER', 'wateruser'),
    'password': os.getenv('WATER_DB_PASSWORD', 'waterpass')
}

def notify_failure(context):
    task_instance = context['task_instance']
    exception = context.get('exception')
    logger.error(f"❌ TAREA FALLIDA: {task_instance.task_id}")
    logger.error(f"🔴 Error: {exception}")
    print(f"🚨 NOTIFICACIÓN: Tarea {task_instance.task_id} falló")

default_args = {
    "owner": "gerardo",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure
}

with DAG(
    dag_id="quality_water_etl",
    start_date=datetime(2025, 11, 23),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    description="ETL Pipeline para análisis de calidad del agua en México",
    tags=["water-quality", "environmental", "etl"]
) as dag:

    def extract():
        try:
            logger.info("🔍 Iniciando extracción...")
            
            if not os.path.exists(RAW_FILE):
                logger.error(f"❌ Archivo no encontrado: {RAW_FILE}")
                raise FileNotFoundError(f"{RAW_FILE} no existe!")
            
            file_size = os.path.getsize(RAW_FILE)
            logger.info(f"✅ Archivo encontrado - Tamaño: {file_size:,} bytes")
            
        except Exception as e:
            logger.error(f"❌ Error en extracción: {str(e)}")
            raise

    def transform():
        try:
            logger.info("🔄 Iniciando transformación...")
            
            import pandas as pd
            
            file_size = os.path.getsize(RAW_FILE)
            
            # SCALING CONSIDERATION: Chunks para archivos grandes
            if file_size > 5 * 1024 * 1024:
                logger.info(f"⚙️ ESCALABILIDAD: Archivo grande ({file_size:,} bytes)")
                logger.info("📦 Procesando en chunks...")
                
                chunk_size = 10000
                chunks = []
                for i, chunk in enumerate(pd.read_csv(RAW_FILE, chunksize=chunk_size), 1):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"✅ {len(df):,} registros procesados")
            else:
                logger.info("⚡ Procesamiento directo")
                df = pd.read_csv(RAW_FILE)
            
            temp_file = RAW_FILE + ".temp"
            df.to_csv(temp_file, index=False)
            
            transform_quality_data(temp_file, TRANSFORMED_FILE_CSV)
            
            # SCALING: Formato Parquet
            df_transformed = pd.read_csv(TRANSFORMED_FILE_CSV)
            df_transformed.to_parquet(TRANSFORMED_FILE_PARQUET, index=False, compression='snappy')
            logger.info("✅ Formato Parquet generado")
            
            os.remove(temp_file)
            logger.info("✅ Transformación completada")
            
        except Exception as e:
            logger.error(f"❌ Error en transformación: {str(e)}")
            raise

    def load():
        try:
            logger.info("📊 Iniciando carga a base de datos...")
            
            import pandas as pd
            from sqlalchemy import create_engine, text
            
            df = pd.read_parquet(TRANSFORMED_FILE_PARQUET)
            logger.info(f"✅ Datos leídos: {len(df):,} registros")
            
            connection_string = (
                f"postgresql+psycopg2://{WATER_DB_CONFIG['user']}:{WATER_DB_CONFIG['password']}"
                f"@{WATER_DB_CONFIG['host']}:{WATER_DB_CONFIG['port']}/{WATER_DB_CONFIG['database']}"
            )
            
            logger.info(f"🔌 Conectando a {WATER_DB_CONFIG['host']}:{WATER_DB_CONFIG['port']}")
            engine = create_engine(connection_string)
            
            # Verificar conexión
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"✅ Conexión exitosa: {version[:50]}...")
            
            # Preparar datos
            df_to_load = df.copy()
            df_to_load['fecha_carga'] = datetime.now()
            
            # Eliminar vistas antes de reemplazar tabla (para evitar conflicto)
            logger.info("🗑️ Preparando base de datos...")
            with engine.begin() as conn:
                conn.execute(text("DROP VIEW IF EXISTS water_data.resumen_por_estado CASCADE;"))
                conn.execute(text("DROP VIEW IF EXISTS water_data.calidad_por_periodo CASCADE;"))
            
            # Cargar datos (replace ahora funcionará)
            logger.info("💾 Insertando datos en water_data.calidad_agua_clean...")
            df_to_load.to_sql(
                name='calidad_agua_clean',
                schema='water_data',
                con=engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Recrear vistas después de cargar datos
            logger.info("📊 Recreando vistas de análisis...")
            with engine.begin() as conn:
                # Vista de resumen por estado
                conn.execute(text("""
                    CREATE VIEW water_data.resumen_por_estado AS
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
                """))
                
                # Vista de calidad por periodo
                conn.execute(text("""
                    CREATE VIEW water_data.calidad_por_periodo AS
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
                """))
            
            # Verificar carga
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM water_data.calidad_agua_clean"))
                count = result.fetchone()[0]
                logger.info(f"✅ Registros en BD: {count:,}")
                
                if 'semaforo' in df.columns:
                    stats_query = """
                    SELECT semaforo, COUNT(*) as total 
                    FROM water_data.calidad_agua_clean 
                    GROUP BY semaforo
                    ORDER BY total DESC
                    """
                    result = conn.execute(text(stats_query))
                    logger.info("\n🚦 Distribución por semáforo:")
                    for row in result:
                        logger.info(f"   {row[0]}: {row[1]:,}")
            
            logger.info("="*60)
            logger.info("✅ CARGA COMPLETADA EXITOSAMENTE")
            logger.info(f"✓ Base de datos: {WATER_DB_CONFIG['database']}")
            logger.info(f"✓ Tabla: water_data.calidad_agua_clean")
            logger.info(f"✓ Registros: {len(df):,}")
            logger.info(f"✓ Vistas recreadas: 2")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"❌ Error en carga: {str(e)}")
            logger.error(f"Configuración BD: {WATER_DB_CONFIG}")
            raise

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3