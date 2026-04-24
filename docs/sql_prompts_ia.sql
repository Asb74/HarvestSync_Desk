-- HarvestSync Desk: configuración de prompts IA por task/cultivo/variedad/version

CREATE TABLE IF NOT EXISTS prompts_ia (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task TEXT NOT NULL,
    cultivo TEXT NOT NULL,
    variedad TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    nombre TEXT,
    descripcion TEXT,
    texto_prompt TEXT NOT NULL,
    activo INTEGER DEFAULT 1,
    es_default INTEGER DEFAULT 0,
    fecha_creacion TEXT,
    fecha_actualizacion TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_prompts_ia_task_cultivo_variedad_version
    ON prompts_ia(task, cultivo, variedad, prompt_version);
CREATE INDEX IF NOT EXISTS idx_prompts_task ON prompts_ia(task);
CREATE INDEX IF NOT EXISTS idx_prompts_cultivo ON prompts_ia(cultivo);
CREATE INDEX IF NOT EXISTS idx_prompts_variedad ON prompts_ia(variedad);
CREATE INDEX IF NOT EXISTS idx_prompts_activo ON prompts_ia(activo);

-- Seed inicial (insertar solo si la tabla está vacía)
INSERT INTO prompts_ia (
    task, cultivo, variedad, prompt_version, nombre, descripcion, texto_prompt,
    activo, es_default, fecha_creacion, fecha_actualizacion
)
SELECT
    'estimacion_calibres',
    'CITRICOS',
    'VALENCIA DELTA',
    'v2_corrige_sesgo_valencia_delta',
    'Estimación cítricos Valencia Delta (corrección sesgo)',
    'Ajustado para corregir sobreestimación CAL8/CAL9 e infraestimación CAL4/CAL5.',
    'Eres un asistente experto en estimación visual agrícola para cítricos en box.\nVariedad objetivo: VALENCIA DELTA.\nObjetivo: estimar distribución APROXIMADA de calibres por foto.\nCorrige explícitamente el sesgo histórico: evitar sobreestimación de CAL8/CAL9, evitar infraestimación de CAL4/CAL5, y manejar oclusión media/frutos parcialmente visibles.\nDevuelve JSON estricto con: apta_para_estimacion, confianza, frutos_visibles_estimados, calibre_dominante, distribucion, advertencias, resumen.\nReglas: suma ~100 y no inventar calibres fuera del contexto.',
    1,
    0,
    datetime('now'),
    datetime('now')
WHERE NOT EXISTS (SELECT 1 FROM prompts_ia)
UNION ALL
SELECT
    'estimacion_calibres',
    'CITRICOS',
    '*',
    'v1_citricos_generico',
    'Estimación cítricos genérico',
    'Prompt base para cítricos cuando no hay ajuste específico de variedad.',
    'Eres un asistente experto en estimación visual agrícola para cítricos en box.\nObjetivo: estimar distribución APROXIMADA de calibres por foto considerando oclusión y perspectiva.\nUsa solo rangos de contexto.\nDevuelve JSON estricto con: apta_para_estimacion, confianza, frutos_visibles_estimados, calibre_dominante, distribucion, advertencias, resumen.',
    1,
    1,
    datetime('now'),
    datetime('now')
WHERE NOT EXISTS (SELECT 1 FROM prompts_ia)
UNION ALL
SELECT
    'estimacion_calibres',
    '*',
    '*',
    'v1_generico_calibres',
    'Estimación calibres genérico',
    'Prompt genérico para cualquier cultivo sin configuración específica.',
    'Eres un asistente de estimación visual agrícola en imágenes de fruta en box.\nObjetivo: estimar distribución aproximada de calibres con cautela y advertencias claras.\nUsa únicamente los rangos del contexto cuando estén disponibles.\nDevuelve JSON estricto con: apta_para_estimacion, confianza, frutos_visibles_estimados, calibre_dominante, distribucion, advertencias, resumen.',
    1,
    1,
    datetime('now'),
    datetime('now')
WHERE NOT EXISTS (SELECT 1 FROM prompts_ia);
