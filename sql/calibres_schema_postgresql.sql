-- Esquema inicial mínimo para calibres (PostgreSQL)

create table if not exists sesiones_analisis (
    id_sesion uuid primary key,
    fecha_inicio timestamp not null default now(),
    fecha_fin timestamp null,
    boleta varchar(80) not null,
    id_muestra varchar(120) not null,
    cultivo varchar(120) null,
    diametro_patron_mm numeric(10,3) not null,
    pantalla_objetivo varchar(120) not null,
    total_fotos integer not null default 0,
    total_fotos_validas integer not null default 0,
    estado varchar(40) not null default 'completada',
    origen_app varchar(80) not null default 'HarvestSync Desk'
);

create index if not exists idx_sesiones_muestra on sesiones_analisis (id_muestra);
create index if not exists idx_sesiones_boleta_fecha on sesiones_analisis (boleta, fecha_inicio desc);

create table if not exists fotos_analizadas (
    id_foto_analizada bigserial primary key,
    id_sesion uuid not null references sesiones_analisis(id_sesion) on delete cascade,
    id_foto_externa varchar(120) not null,
    ruta_local varchar(600) not null,
    url_origen varchar(1000) null,
    timestamp_captura timestamp null,
    checksum_sha256 varchar(64) null,
    ancho_px integer null,
    alto_px integer null,
    seleccionada boolean not null default true,
    unique (id_sesion, id_foto_externa)
);

create index if not exists idx_fotos_sesion on fotos_analizadas (id_sesion);
create index if not exists idx_fotos_ruta on fotos_analizadas (ruta_local);

create table if not exists detecciones_patron (
    id_deteccion bigserial primary key,
    id_foto_analizada bigint not null references fotos_analizadas(id_foto_analizada) on delete cascade,
    detectado boolean not null,
    diametro_px numeric(12,4) null,
    mm_por_pixel numeric(12,6) null,
    centro_x_px numeric(12,4) null,
    centro_y_px numeric(12,4) null,
    valida_para_siguiente_paso boolean not null,
    error_texto text null,
    overlay_path varchar(700) null,
    fecha_deteccion timestamp not null default now(),
    parametros_json jsonb null
);

create index if not exists idx_detecciones_foto on detecciones_patron (id_foto_analizada);
create index if not exists idx_detecciones_valida on detecciones_patron (valida_para_siguiente_paso);

create table if not exists frutos_detectados (
    id_fruto bigserial primary key,
    id_foto_analizada bigint not null references fotos_analizadas(id_foto_analizada) on delete cascade,
    indice_fruto integer not null,
    bbox_x integer not null,
    bbox_y integer not null,
    bbox_w integer not null,
    bbox_h integer not null,
    diametro_px numeric(12,4) null,
    diametro_mm numeric(12,4) null,
    calibre_estimado varchar(80) null,
    confianza numeric(5,4) null,
    etiqueta_manual varchar(80) null,
    fecha_registro timestamp not null default now(),
    unique (id_foto_analizada, indice_fruto)
);

create index if not exists idx_frutos_foto on frutos_detectados (id_foto_analizada);
create index if not exists idx_frutos_etiqueta on frutos_detectados (etiqueta_manual);

create table if not exists correcciones_manuales (
    id_correccion bigserial primary key,
    id_deteccion bigint null references detecciones_patron(id_deteccion) on delete set null,
    id_fruto bigint null references frutos_detectados(id_fruto) on delete set null,
    campo_corregido varchar(80) not null,
    valor_anterior text null,
    valor_nuevo text not null,
    motivo varchar(200) null,
    usuario varchar(120) not null,
    fecha_correccion timestamp not null default now()
);

create index if not exists idx_correcciones_deteccion_fecha on correcciones_manuales (id_deteccion, fecha_correccion desc);
create index if not exists idx_correcciones_fruto_fecha on correcciones_manuales (id_fruto, fecha_correccion desc);
