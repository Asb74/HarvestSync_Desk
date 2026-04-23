# Persistencia SQL central - HarvestSync Desk (calibres)

## Motor recomendado

**Elección: PostgreSQL (servidor central en red local).**

### Justificación para el contexto actual

- **Windows + red local:** PostgreSQL se instala y administra sin complejidad excesiva en Windows Server/Windows 10-11 (servicio estable, backups simples con `pg_dump`).
- **Concurrencia:** mejor comportamiento que SQLite compartido para escrituras concurrentes de varias estaciones.
- **Mantenibilidad:** esquema fuerte, buenas constraints, índices y trazabilidad para auditoría de correcciones.
- **Escalabilidad incremental:** permite crecer desde la fase de patrón hasta dataset de frutos sin rediseños extremos.
- **Coste operativo:** licencia libre, herramientas maduras, sin coste por core/cliente.

> SQL Server Express también es válido en entorno Windows, pero PostgreSQL evita límites de edición Express y mantiene costes de operación bajos para histórico analítico.

## Arquitectura lógica recomendada

1. **Servidor de archivos** (compartido de red):
   - Guarda imágenes originales.
   - Guarda overlays temporales de validación visual en carpeta dedicada.
2. **Servidor PostgreSQL**:
   - Guarda sesiones, fotos analizadas, resultados de detección, frutos futuros y correcciones.
3. **Cliente Python/Tkinter**:
   - Sigue leyendo fotos desde ruta/URL del servidor.
   - Inserta metadatos/resultados en SQL mediante repositorio desacoplado.

## Carpeta de overlays propuesta

- Ruta temporal controlada por estación: `%TEMP%/harvestsync_desk/calibres_overlays`.
- Ventajas:
  - No contamina el repositorio ni las rutas de imágenes originales.
  - Permite depuración local y limpieza por TTL.
  - Evita bloqueo de flujo si el servidor de archivos está temporalmente lento.

## Integración Python mantenible

- Crear un módulo repositorio `calibres_sql_repository.py` con operaciones explícitas:
  - `crear_sesion_analisis(...)`
  - `registrar_foto_analizada(...)`
  - `registrar_deteccion_patron(...)`
  - `registrar_frutos_detectados(...)` (fase futura)
  - `registrar_correccion_manual(...)` (fase futura)
- Usar transacciones cortas por lote de detección.
- Pasar `connection string` por variable de entorno o archivo de configuración existente (sin hardcode de credenciales).

## Índices clave

- Búsqueda por sesión: `fotos_analizadas(id_sesion)`.
- Búsqueda por muestra: `sesiones_analisis(id_muestra)`.
- Dataset para entrenamiento futuro: `frutos_detectados(id_foto, etiqueta_manual)`.
- Auditoría de calidad: `correcciones_manuales(id_deteccion, fecha_correccion)`.
