import io
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.pagesizes import landscape  # Asegúrate de tenerlo importado
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("HarvestSync.json")
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

styles = getSampleStyleSheet()


def generar_pdf_general(lista_datos):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4))
    elementos = []

    ahora = datetime.now().strftime('%d/%m/%Y %H:%M')
    elementos.append(Paragraph("<b>Informe General Agrupado</b>", styles['Title']))
    elementos.append(Spacer(1, 12))
    elementos.append(Paragraph(f"Fecha de generación: {ahora}", styles['Normal']))
    elementos.append(Spacer(1, 12))

    agrupado = {}
    for item in lista_datos:
        cultivo = item["CULTIVO"]
        if cultivo not in agrupado:
            agrupado[cultivo] = []
        agrupado[cultivo].append(item)

    for cultivo, muestras in agrupado.items():
        elementos.append(Paragraph(f"<b>Cultivo: {cultivo}</b>", styles['Heading2']))
        elementos.append(Spacer(1, 6))

        # === Tabla inicial de muestras ===
        encabezado_muestra = ["Boleta", "Nombre", "Variedad", "IdMuestra", "Albaran", "FechaHora", "Tipo"]
        filas_muestra = [encabezado_muestra]

        for muestra in muestras:
            id_muestra = muestra["IdMuestra"]
            datos = db.collection("Muestras").document(id_muestra).get().to_dict()

            boleta = str(muestra.get("Boleta", ""))
            nombre = muestra.get("Nombre", "")
            variedad = db.collection("EEPP").document(boleta).get().to_dict()
            variedad = variedad.get("Variedad", "-") if variedad else "-"

            fila = [
                boleta,
                nombre,
                variedad,
                id_muestra,
                datos.get("Albaran", "-"),
                datos.get("FechaHora", "").strftime('%d/%m/%Y %H:%M') if hasattr(datos.get("FechaHora", ""), 'strftime') else "-",
                datos.get("Tipo", "-")
            ]
            filas_muestra.append(fila)

        # Sin totalizar columnas de string como boleta
        tabla_muestra = Table(filas_muestra, repeatRows=1)
        tabla_muestra.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
        ]))
        elementos.append(Paragraph("<b><i>Datos Muestra</i></b>", styles['Heading3']))
        elementos.append(tabla_muestra)
        elementos.append(Spacer(1, 8))

        # === Secciones dinámicas ===
        doc_plantilla = db.collection("PlantillasInforme").document("DATOS").get().to_dict()
        secciones = [s for s in doc_plantilla.get("CAMPO", []) if s != "PlantillasMuestra"]


        for seccion in secciones:
            doc_seccion = db.collection(seccion).document(cultivo).get().to_dict()
            if not doc_seccion:
                continue

            titulo = doc_seccion.get("Titulo", seccion)
            campos_raw = doc_seccion.get("CAMPO", [])
            campos = [c.split("[")[0].strip() for c in campos_raw]

            tabla_datos = [ ["Boleta", "Nombre"] + campos ]
            datos_validos = []

            for muestra in muestras:
                id_muestra = muestra["IdMuestra"]
                datos = db.collection("Muestras").document(id_muestra).get().to_dict()
                if not datos:
                    continue

                fila = [str(muestra.get("Boleta", "")), muestra.get("Nombre", "")]
                fila_valores = []
                valores_presentes = False

                for campo in campos:
                    valor = datos.get(campo, "")
                    fila_valores.append(valor)
                    if valor not in ["", "-", None]:
                        valores_presentes = True

                if valores_presentes:
                    fila.extend(str(v) if v not in [None, "-"] else "-" for v in fila_valores)
                    tabla_datos.append(fila)
                    datos_validos.append((fila[2:]))

            if len(tabla_datos) <= 1:
                continue

            # Calcular promedios solo si hay datos válidos
            if datos_validos:
                try:
                    import pandas as pd
                    df = pd.DataFrame(datos_validos, columns=campos)
                    df = df.apply(pd.to_numeric, errors='coerce')
                    promedios = df.mean(skipna=True).round(1).fillna("-")
                    fila_total = ["TOTAL", ""] + [f"{val:.1f}" if isinstance(val, float) else "-" for val in promedios]
                    tabla_datos.append(fila_total)
                except Exception as e:
                    print(f"⚠️ Error calculando promedio: {e}")

            tabla = Table(tabla_datos, repeatRows=1)
            tabla.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold')
            ]))

            elementos.append(Paragraph(f"<b><i>{titulo}</i></b>", styles['Heading3']))
            elementos.append(tabla)
            elementos.append(Spacer(1, 12))

    doc.build(elementos)
    buffer.seek(0)
    return buffer
