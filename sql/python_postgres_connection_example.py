"""Ejemplo mínimo de conexión PostgreSQL para HarvestSync Desk.

Requiere instalar psycopg[binary] en el entorno de ejecución.
No se hardcodean credenciales: usar variable de entorno.
"""
from __future__ import annotations

import os

import psycopg


class CalibresSqlRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def crear_sesion_analisis(self, payload: dict) -> None:
        sql = """
            insert into sesiones_analisis (
                id_sesion, boleta, id_muestra, cultivo, diametro_patron_mm,
                pantalla_objetivo, total_fotos, total_fotos_validas, estado
            ) values (
                %(id_sesion)s, %(boleta)s, %(id_muestra)s, %(cultivo)s, %(diametro_patron_mm)s,
                %(pantalla_objetivo)s, %(total_fotos)s, %(total_fotos_validas)s, %(estado)s
            )
        """
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
            conn.commit()


def build_repository_from_env() -> CalibresSqlRepository:
    dsn = os.getenv("HARVESTSYNC_CALIBRES_DSN", "")
    if not dsn:
        raise RuntimeError("Defina HARVESTSYNC_CALIBRES_DSN antes de usar el repositorio SQL.")
    return CalibresSqlRepository(dsn)
