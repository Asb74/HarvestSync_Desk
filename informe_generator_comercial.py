from reportlab.lib.pagesizes import landscape, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import cm
from firebase_admin import firestore
from collections import defaultdict
import statistics

from pdf_utils import create_temp_pdf_name, open_pdf

SECCIONES_UTILIZADAS = ['Datos Calibre', 'Aprovechamiento']

# Acceso a Firestore
DB = firestore.client()

def obtener_variedad(boleta):
    doc = DB.collection('EEPP').document(boleta).get()
    return doc.to_dict().get('Variedad', '') if doc.exists else ''

def calcular_media(valores):
    valores_numericos = [v for v in valores if isinstance(v, (int, float))]
    return round(statistics.mean(valores_numericos), 2) if valores_numericos else ''

def generar_informe_comercial_desde_ui(lista_datos, nombre: str | None = None):
    filename = create_temp_pdf_name(nombre, prefix="InformeComercial")
    doc = SimpleDocTemplate(filename, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20)
    styles = getSampleStyleSheet()
    elementos = []

    # Agrupar datos por Cultivo > Boleta
    datos_agrupados = defaultdict(lambda: defaultdict(list))
    for muestra in lista_datos:
        cultivo = muestra.get('CULTIVO', 'Desconocido')
        boleta = muestra.get('Boleta', 'Sin Boleta')
        datos_agrupados[cultivo][boleta].append(muestra)

    for cultivo, boletas in sorted(datos_agrupados.items()):
        elementos.append(Paragraph(f"<b>CULTIVO: {cultivo}</b>", styles['Heading2']))
        elementos.append(Spacer(1, 0.3 * cm))

        # Tabla resumen por boleta
        tabla_boletas = [["Boleta", "Nombre", "Variedad", "Kg"]]
        resumen_boletas = {}
        for boleta, muestras in sorted(boletas.items()):
            nombre = muestras[0].get('Nombre', '')
            variedad = obtener_variedad(boleta)
            kg_total = sum([m.get('Kg', 0) or 0 for m in muestras])
            tabla_boletas.append([boleta, nombre, variedad, round(kg_total, 2)])
            resumen_boletas[boleta] = {'Nombre': nombre, 'Kg': kg_total, 'Muestras': muestras}

        t_boletas = Table(tabla_boletas, repeatRows=1)
        t_boletas.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ]))
        elementos.append(t_boletas)
        elementos.append(Spacer(1, 0.5 * cm))

        # Secciones: Datos Calibre, Aprovechamiento
        for seccion in SECCIONES_UTILIZADAS:
            # Recolectar todos los campos usados en esta seccion
            campos = set()
            for datos in resumen_boletas.values():
                for muestra in datos['Muestras']:
                    seccion_data = muestra.get(seccion, {})
                    campos.update(seccion_data.keys())
            campos = sorted(list(campos))

            if not campos:
                continue

            elementos.append(Paragraph(f"{seccion.upper()}", styles['Heading3']))

            tabla_seccion = [[campo for campo in campos]]
            for boleta, datos in resumen_boletas.items():
                fila = []
                for campo in campos:
                    valores = [m.get(seccion, {}).get(campo, '') for m in datos['Muestras']]
                    media = calcular_media(valores)
                    fila.append(media)
                tabla_seccion.append(fila)

            t_seccion = Table(tabla_seccion, repeatRows=1)
            t_seccion.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ]))
            elementos.append(t_seccion)
            elementos.append(Spacer(1, 0.5 * cm))

    doc.build(elementos)
    open_pdf(filename)
    return filename
