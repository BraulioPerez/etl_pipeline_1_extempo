import pandas as pd

def transform_quality_data(input_path: str, output_path: str):
    """
    Transforma datos de calidad del agua del monitoreo de CONAGUA
    """
    df = pd.read_csv(input_path)
    
    print(f"📊 Datos cargados: {len(df)} registros")
    print(f"📋 Columnas originales: {len(df.columns)}")
    
    # Limpieza básica
    df = df.drop_duplicates()
    print(f"✓ Duplicados eliminados. Registros restantes: {len(df)}")
    
    # Estandarizar nombres de columnas
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
    
    # Rellenar valores nulos
    df.fillna(0, inplace=True)
    
    # Mapeo de calidad del agua según los valores reales del CSV
    # Valores encontrados: "Excelente", "Buena calidad", "Aceptable", "Contaminada"
    quality_map = {
        "Excelente": 1, 
        "Buena calidad": 2, 
        "Aceptable": 3, 
        "Contaminada": 4
    }
    
    # Crear índice de calidad basado en DQO (Demanda Química de Oxígeno)
    if 'calidad_dqo' in df.columns:
        df['indice_calidad_dqo'] = df['calidad_dqo'].map(quality_map)
        print(f"✓ Índice de calidad DQO creado")
        
        # Mostrar distribución
        print("\n📈 Distribución de calidad DQO:")
        print(df['calidad_dqo'].value_counts())
    else:
        print("⚠ Advertencia: Columna 'calidad_dqo' no encontrada")
    
    # Crear índice de calidad basado en DBO (Demanda Bioquímica de Oxígeno)
    if 'calidad_dbo' in df.columns:
        df['indice_calidad_dbo'] = df['calidad_dbo'].map(quality_map)
        print(f"✓ Índice de calidad DBO creado")
    
    # Crear índice de calidad basado en SST (Sólidos Suspendidos Totales)
    if 'calidad_sst' in df.columns:
        df['indice_calidad_sst'] = df['calidad_sst'].map(quality_map)
        print(f"✓ Índice de calidad SST creado")
    
    # Crear un índice de calidad general (promedio de los índices disponibles)
    indice_columns = [col for col in df.columns if col.startswith('indice_calidad_')]
    if indice_columns:
        df['indice_calidad_general'] = df[indice_columns].mean(axis=1).round(2)
        print(f"✓ Índice de calidad general creado (promedio de {len(indice_columns)} índices)")
    
    # Agregar categoría de semáforo como numérica
    semaforo_map = {"VERDE": 1, "AMARILLO": 2, "ROJO": 3}
    if 'semaforo' in df.columns:
        df['semaforo_numerico'] = df['semaforo'].map(semaforo_map)
        print(f"✓ Semáforo convertido a numérico")
        print("\n🚦 Distribución de semáforo:")
        print(df['semaforo'].value_counts())
    
    # Guardar archivo transformado
    df.to_csv(output_path, index=False)
    print(f"\n✅ Datos transformados guardados en: {output_path}")
    print(f"✅ Total de registros: {len(df)}")
    print(f"✅ Total de columnas: {len(df.columns)}")
    
    # Mostrar resumen estadístico
    print("\n📊 Resumen de índices de calidad:")
    for col in indice_columns + ['indice_calidad_general']:
        if col in df.columns:
            print(f"{col}: media={df[col].mean():.2f}, min={df[col].min()}, max={df[col].max()}")
    
    return output_path