import io
import requests
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, Flowable
from reportlab.lib.styles import getSampleStyleSheet
import firebase_admin
from firebase_admin import credentials, firestore
from reportlab.lib.colors import HexColor
import threading
from tkinter import Toplevel, Label

mi_color = HexColor("#7D98A1")

cred = credentials.Certificate("HarvestSync.json")

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

db = firestore.client()

styles = getSampleStyleSheet()


def _es_grafica_posible(filas):
    try:
        valores = [float(valor) for _, valor in filas]
        suma = sum(valores)
        return 99.5 < suma < 100.5
    except:
        return False


from reportlab.lib.styles import ParagraphStyle

def _crear_grafica(filas):
    titulo_grafico_style = ParagraphStyle(name='GraficoTitulo', fontSize=14, leading=16, fontName='Helvetica-Bold')
    grafico = [Paragraph("Gráfico de distribución", titulo_grafico_style), Spacer(1, 6)]

    for etiqueta, valor in filas:
        try:
            porcentaje = float(valor)
        except:
            continue

        ancho_barra = 10 * cm * (porcentaje / 100)
        fila = Table(
            [[
                Paragraph(etiqueta, styles['Normal']),
                Table(
                    [[
                        '',
                        Paragraph(f"{porcentaje:.1f}%", styles['Normal'])
                    ]],
                    colWidths=[ancho_barra, 2 * cm],
                    style=TableStyle([
                    ('BACKGROUND', (0, 0), (0, 0), mi_color),
                    ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),     # Espacio entre barra y borde
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),    # Espacio entre barra y porcentaje
                    ('TOPPADDING', (0, 0), (-1, -1), 1),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 1),
                ])
                )
            ]],
            colWidths=[3 * cm, 13 * cm],
            style=TableStyle([('VALIGN', (0, 0), (-1, -1), 'MIDDLE')])
        )

        grafico.append(fila)
        grafico.append(Spacer(1, 1))

    return grafico
def generar_con_espera():
    loading = Toplevel(root)
    loading.title("Generando informe...")
    Label(loading, text="Por favor, espere. Generando informe PDF...").pack(padx=20, pady=20)
    loading.update()

    def tarea():
        try:
            buffer = generar_pdf(id_muestra, cultivo, uid_usuario)
            guardar_pdf(buffer)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo generar el informe:\n{e}")
        loading.destroy()

    threading.Thread(target=tarea).start()


def generar_pdf(id_muestra, cultivo, uid_usuario):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elementos = []

    datos_muestra = db.collection("Muestras").document(id_muestra).get().to_dict()
    plantilla_datos = db.collection("PlantillasInforme").document("DATOS").get().to_dict()
    secciones = plantilla_datos.get("CAMPO", [])

    usuario_doc = db.collection("UsuariosAutorizados").document(uid_usuario).get().to_dict()
    nombre_usuario = usuario_doc.get("Nombre", uid_usuario)

    from urllib.parse import quote

    servidor_doc = db.collection("ServidorFotos").document("url_actual").get().to_dict()
    url_base = servidor_doc.get("url", "")
    ruta_carpeta = servidor_doc.get("carpeta", "")
    carpeta_codificada = quote(ruta_carpeta)


    # Agregar logo
    try:
        elementos.append(Image("COOPERATIVA.png", width=6*cm))
    except:
        pass

    elementos.append(Spacer(1, 12))
    elementos.append(Paragraph("<b>Informe de Muestra</b>", styles['Title']))
    elementos.append(Spacer(1, 12))
    elementos.append(Paragraph(f"ID Muestra: {id_muestra}", styles['Normal']))
    elementos.append(Paragraph(f"Cultivo: {cultivo}", styles['Normal']))
    elementos.append(Paragraph(f"Usuario: {nombre_usuario}", styles['Normal']))
    elementos.append(Paragraph(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
    elementos.append(Spacer(1, 12))

    for seccion in secciones:
        doc_seccion = db.collection(seccion).document(cultivo).get().to_dict()
        if not doc_seccion:
            continue

        titulo = doc_seccion.get("Titulo", seccion)
        campos_raw = doc_seccion.get("CAMPO", [])
        campos = [c.split("[")[0].strip() for c in campos_raw]

        datos_tabla = []
        for campo in campos:
            valor = datos_muestra.get(campo, "-")
            if hasattr(valor, 'isoformat'):
                valor = valor.strftime('%d/%m/%Y %H:%M')
            datos_tabla.append([campo, str(valor)])

        elementos.append(Paragraph(f"<b>{titulo}</b>", styles['Heading2']))
        tabla = Table(datos_tabla, colWidths=[6*cm, 10*cm])
        tabla.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP')
        ]))
        elementos.append(tabla)

        if _es_grafica_posible(datos_tabla):
            elementos.append(Spacer(1, 6))
            elementos.extend(_crear_grafica(datos_tabla))
            elementos.append(Spacer(1, 18))

        else:
            elementos.append(Spacer(1, 12))

        # Obtener ruta del servidor para query
        config_doc = db.collection("ServidorFotos").document("confingSalida").get().to_dict()
        if not config_doc:
            raise ValueError("El documento 'configSalida' no existe o está vacío en la colección 'ServidorFotos'.")
        ruta_servidor = config_doc.get("rutaservidor", "")
        carpeta_encoded = requests.utils.quote(ruta_servidor)

        print(f"🧾 Carpeta codificada: {carpeta_encoded}")

        fotos = db.collection("Fotos").where("idMuestra", "==", id_muestra).where("pantalla", "==", titulo).order_by("timestamp").stream()
        imagenes = []
        for foto_doc in fotos:
            ruta = foto_doc.to_dict().get("ruta_local", "")
            if ruta.endswith(".jpg"):
                url_completa = f"{url_base}/fotos/{ruta}?carpeta={carpeta_encoded}"
                print(f"📷 Intentando descargar: {url_completa}")
                try:
                    resp = requests.get(url_completa)
                    if resp.status_code == 200:
                        img = Image(io.BytesIO(resp.content), width=4 * cm, height=4 * cm)
                        imagenes.append(img)
                    else:
                        print(f"❌ Error HTTP {resp.status_code} al descargar imagen.")
                except Exception as e:
                    print(f"❌ Excepción descargando imagen: {e}")

        if imagenes:
            elementos.append(Spacer(1, 6))

            # Organizar en filas de hasta 4 fotos
            filas = [imagenes[i:i+4] for i in range(0, len(imagenes), 4)]

            # Rellenar cada fila con espacios vacíos si tiene menos de 4 imágenes
            for fila in filas:
                while len(fila) < 4:
                    fila.append(Spacer(4.5*cm, 4.5*cm))

            tabla_fotos = Table(
                filas,
                colWidths=[4.5*cm] * 4,
                hAlign='CENTER'
            )

            tabla_fotos.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('LEFTPADDING', (0, 0), (-1, -1), 6),
                ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))

            elementos.append(tabla_fotos)
            elementos.append(Spacer(1, 4))

        #if imagenes:
        #    elementos.append(Spacer(1, 6))
        #    elementos.extend(imagenes)
        #    elementos.append(Spacer(1, 12))

    doc.build(elementos)
    buffer.seek(0)
    return buffer


# Ejemplo de uso:
# with open("informe_demo.pdf", "wb") as f:
#     f.write(generar_pdf("ID", "CULTIVO", "UID").read())
