# HarvestSync - Integración OpenAI vía servicio interno (fase incremental)

## plan

1. **Separar responsabilidades cliente-servidor**
   - El cliente HarvestSync Desk solo envía solicitudes al servidor interno.
   - El servidor interno concentra llamadas a OpenAI y custodia secretos.
2. **Crear servicio Python mínimo y mantenible**
   - Flask por simplicidad operativa (arranque rápido, poco boilerplate).
   - Endpoints iniciales: `/health` y `/analyze-image`.
3. **Aplicar seguridad mínima práctica**
   - API key solo en `C:\ProgramData\HarvestSync\Secret\openai_key.txt`.
   - Permisos NTFS restrictivos.
   - Token interno opcional + whitelist IP opcional.
4. **Despliegue incremental en servidor Windows**
   - Carpeta dedicada para servicio.
   - Variables de entorno para parametrizar.
   - Arranque inicial manual y posterior como tarea/servicio.
5. **Pruebas y rollback claros**
   - Smoke test de endpoints.
   - Manejo de errores previstos.
   - Capacidad de desactivar integración sin romper HarvestSync Desk.

## cambios

### 1) Arquitectura propuesta (MVP)

**Componentes**
- `HarvestSync Desk (cliente)`
- `HarvestSync Internal AI Service (servidor interno, Flask)`
- `OpenAI API (externo)`

**Flujo de petición**
1. El cliente necesita análisis IA para una imagen ya disponible en red/servidor.
2. El cliente llama `POST /analyze-image` al servicio interno (sin API key).
3. El servicio interno valida request y existencia de imagen.
4. El servicio interno lee la key de `C:\ProgramData\HarvestSync\Secret\openai_key.txt`.
5. El servicio interno llama a OpenAI.
6. El servicio interno devuelve respuesta estructurada al cliente.

**Ubicación de la clave**
- Solo servidor interno.
- Ruta objetivo: `C:\ProgramData\HarvestSync\Secret\openai_key.txt`.
- Nunca en clientes, nunca en carpeta de imágenes.

### 2) Servicio interno implementado

Carpeta: `internal_ai_service/`
- `app.py`: API Flask (`/health`, `/analyze-image`) con validaciones.
- `config.py`: settings y variables de entorno.
- `openai_gateway.py`: lectura de key y llamada HTTP a OpenAI.
- `requirements-service.txt`: dependencia mínima Flask.

### 3) Endpoints

#### `GET /health`
Respuesta ejemplo:
```json
{
  "ok": true,
  "service": "harvestsync-internal-ai",
  "key_file_exists": true,
  "model": "gpt-4.1-mini"
}
```

#### `POST /analyze-image`
Request ejemplo:
```json
{
  "image_path": "C:/ruta/box_123.jpg",
  "task": "validacion_foto",
  "context": "revisar encuadre y visibilidad"
}
```

Errores gestionados:
- `invalid_json` (400)
- `invalid_image_path` (400)
- `image_not_found` (404)
- `openai_key_file_missing` (500)
- `openai_key_empty` (500)
- `openai_timeout` (504)
- `openai_error` (502)

### 4) Cliente mínimo de integración

Archivo: `client_examples/internal_ai_client.py`
- No usa ni conoce API key.
- Llama al servicio interno con timeout.
- Propaga errores controlados para UI/log del cliente.

### 5) Seguridad mínima práctica recomendada

1. **Clave fuera de cliente**: cumplido por diseño.
2. **Permisos NTFS carpeta secreta**
   - Ruta: `C:\ProgramData\HarvestSync\Secret\`
   - Permitir: `SYSTEM`, `Administrators`, y cuenta de servicio del proceso Python.
   - Denegar lectura a usuarios estándar de puestos cliente.
3. **Autenticación simple opcional**
   - Header `X-Internal-Token` configurado por variable `HARVESTSYNC_INTERNAL_TOKEN`.
4. **Whitelist IP opcional**
   - `HARVESTSYNC_ALLOWED_IPS=192.168.1.10,192.168.1.11`
5. **No loguear secretos**
   - El código no imprime ni serializa la API key.

### 6) Despliegue realista en Windows

1. Crear carpeta secreto:
   - `C:\ProgramData\HarvestSync\Secret\`
2. Guardar API key en:
   - `C:\ProgramData\HarvestSync\Secret\openai_key.txt`
3. Ubicar servicio Python (ejemplo):
   - `C:\HarvestSync\internal_ai_service\`
4. Instalar dependencias del servicio:
   - `pip install -r requirements-service.txt`
5. Variables de entorno recomendadas:
   - `HARVESTSYNC_AI_BIND_HOST=0.0.0.0`
   - `HARVESTSYNC_AI_BIND_PORT=8086`
   - `HARVESTSYNC_OPENAI_KEY_PATH=C:\ProgramData\HarvestSync\Secret\openai_key.txt`
   - `HARVESTSYNC_INTERNAL_TOKEN=<token interno opcional>`
   - `HARVESTSYNC_ALLOWED_IPS=<IPs autorizadas opcional>`
6. Arranque:
   - `python -m internal_ai_service.app`
7. Configuración clientes:
   - URL interna, por ejemplo: `http://SERVIDOR-HARVESTSYNC:8086`

### 7) Rollback claro

Si se requiere retroceso:
1. En cliente, desactivar llamada al servicio interno y volver a flujo previo sin IA.
2. Detener proceso del servicio interno en servidor.
3. Mantener carpeta de secreto sin cambios.
4. No hay cambios destructivos en base de datos ni estructura de imágenes.

## pruebas

### Smoke test manual
1. `GET /health` devuelve `ok=true`.
2. `POST /analyze-image` con imagen válida devuelve `ok=true`.

### Casos borde
1. Renombrar temporalmente `openai_key.txt` => error `openai_key_file_missing`.
2. Dejar archivo vacío => error `openai_key_empty`.
3. Enviar `image_path` inexistente => `image_not_found`.
4. Enviar JSON inválido => `invalid_json`.
5. Simular timeout reduciendo `HARVESTSYNC_OPENAI_TIMEOUT=1` => `openai_timeout`.

### Validación de rollback
1. Detener servicio.
2. Confirmar que cliente no bloquea flujo principal de HarvestSync Desk.

## riesgos

1. **Conectividad OpenAI inestable**
   - Mitigación: timeout controlado + mensajes de error claros + fallback sin IA.
2. **Acceso indebido en red local**
   - Mitigación: token interno, whitelist IP, firewall local de servidor.
3. **Permisos NTFS mal aplicados**
   - Mitigación: checklist de despliegue y validación con cuenta de usuario estándar.
4. **Crecimiento desordenado de endpoints**
   - Mitigación: separar `app/config/gateway` desde el inicio y versionar API interna.
