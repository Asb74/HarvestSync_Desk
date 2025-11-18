# Rediseño moderno de HarvestSync_Desk
# Estructura general: layout modular, uso de ttk.LabelFrame, estilos, y botones con iconos
# Se conserva toda funcionalidad

# A continuación actualizaremos el archivo `HarvestSync_Desk.py` para reflejar una interfaz moderna.
# Cambios principales:
# - Agrupar filtros en un LabelFrame "Filtros de búsqueda"
# - Agrupar acciones en un LabelFrame "Acciones disponibles"
# - Separar secciones por espaciado visual
# - Aplicar estilo `clam`, personalizar fuente y botón
# - Mostrar icono cuadrado de app

# Importaciones
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
from informe_generator import generar_pdf
from informe_generator_general import generar_pdf_general
from informe_generator_comercial import generar_informe_comercial_desde_ui
from herramientas import abrir_herramientas
import sys, os
import traceback
#from tkinter.simpledialog import askstring
from PIL import Image, ImageTk
from tkinter import simpledialog
from pathlib import Path
from pdf_utils import cleanup_old_pdfs

# === Funciones auxiliares ===
def recurso_path(rel_path: str) -> str:
    # 1) cuando está instalado (junto al .exe en Program Files)
    if getattr(sys, 'frozen', False):
        app_dir = Path(sys.executable).parent
        p = app_dir / rel_path
        if p.exists():
            return str(p)
        # 2) si además lo empacaste con --add-data, estará en _MEIPASS
        base = Path(getattr(sys, "_MEIPASS", app_dir))
        return str(base / rel_path)
    # 3) ejecución normal (fuentes)
    return str(Path(__file__).resolve().parent / rel_path)

cred = credentials.Certificate(recurso_path("HarvestSync.json"))


# === Inicializar Firebase ===
cred = credentials.Certificate(recurso_path("HarvestSync.json"))
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

# === Cargar usuarios ===
def cargar_usuarios():
    usuarios = {}
    docs = db.collection("UsuariosAutorizados").stream()
    for doc in docs:
        data = doc.to_dict()
        usuarios[doc.id] = data.get("Nombre", doc.id)
    return usuarios

usuarios_dict = cargar_usuarios()

# === Tkinter Modern UI ===
root = tk.Tk()
root.title("HarvestSync Desk")
root.geometry("1250x700")
try:
    root.state("zoomed")
except tk.TclError:
    try:
        root.attributes("-zoomed", True)
    except tk.TclError:
        pass
style = ttk.Style(root)
style.theme_use("clam")
style.configure("TButton", padding=6, font=("Segoe UI", 10))
style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

# Icono cuadrado
try:
    icon_path = recurso_path("icono_app.png")
    logo_img = Image.open(icon_path).resize((32, 32))
    logo_icon = ImageTk.PhotoImage(logo_img)
    root.iconphoto(False, logo_icon)
except Exception:  # noqa: BLE001 - mantener compatibilidad con ejecuciones empaquetadas
    logo_icon = None

root.logo_icon = logo_icon

# Limpieza de PDFs temporales al iniciar la aplicación
cleanup_old_pdfs(max_age_hours=24)


def _on_close() -> None:
    cleanup_old_pdfs(max_age_hours=24)
    root.destroy()


root.protocol("WM_DELETE_WINDOW", _on_close)
# Función personalizada para askstring con icono
    

def custom_input_window(title, prompt):
    top = tk.Toplevel(root)
    top.title(title)
    if logo_icon:
        top.iconphoto(False, logo_icon)

    tk.Label(top, text=prompt).pack(padx=20, pady=(15, 5))
    entry = ttk.Entry(top, width=30)
    entry.pack(padx=20, pady=5)
    entry.focus()

    result = {"value": None}

    def confirmar():
        result["value"] = entry.get()
        top.destroy()

    def cancelar():
        top.destroy()

    frame_botones = tk.Frame(top, bg=top.cget("bg"))  # Usa el mismo fondo que la ventana
    frame_botones.pack(pady=10)

    ttk.Button(frame_botones, text="OK", command=confirmar).grid(row=0, column=0, padx=5)
    ttk.Button(frame_botones, text="Cancelar", command=cancelar).grid(row=0, column=1, padx=5)

    top.grab_set()  # bloquea la interacción con la ventana principal
    root.wait_window(top)
    return result["value"]



# === Filtros ===
frame_filtros = ttk.LabelFrame(root, text="Filtros de búsqueda")
frame_filtros.pack(padx=10, pady=10, fill="x")

filtros = {}
campos = ["Boleta", "Nombre", "Tipo", "Usuario", "CULTIVO", "Variedad"]

for idx, campo in enumerate(campos):
    ttk.Label(frame_filtros, text=campo).grid(row=0, column=idx, padx=5, pady=2)
    entry = ttk.Entry(frame_filtros, width=15)
    entry.grid(row=1, column=idx, padx=5, pady=2)
    filtros[campo] = entry

ttk.Label(frame_filtros, text="Desde").grid(row=0, column=len(campos), padx=5)
fecha_desde = DateEntry(frame_filtros, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern="dd/mm/yyyy")
fecha_desde.grid(row=1, column=len(campos), padx=5)

ttk.Label(frame_filtros, text="Hasta").grid(row=0, column=len(campos)+1, padx=5)
fecha_hasta = DateEntry(frame_filtros, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern="dd/mm/yyyy")
fecha_hasta.grid(row=1, column=len(campos)+1, padx=5)

# === Acciones ===
frame_botones = ttk.LabelFrame(root, text="Acciones disponibles")
frame_botones.pack(padx=10, pady=10, fill="x")

ttk.Button(frame_botones, text="🔍 Buscar muestras", command=lambda: filtrar()).grid(row=0, column=0, padx=10, pady=6)
ttk.Button(frame_botones, text="📄 Informe muestra seleccionada", command=lambda: generar_informe_seleccionado()).grid(row=0, column=1, padx=10, pady=6)
ttk.Button(frame_botones, text="🗑 Eliminar seleccionados", command=lambda: eliminar_muestras()).grid(row=0, column=2, padx=10, pady=6)
#ttk.Button(frame_botones, text="📦 Informe comercial", command=lambda: ejecutar_informe_comercial()).grid(row=0, column=3, padx=10, pady=6)
ttk.Button(frame_botones, text="📊 Informe general", command=lambda: generar_informe_general()).grid(row=0, column=3, padx=10, pady=6)
ttk.Button(frame_botones, text="🧮 Última Muestra", command=lambda: generar_informe_unico_por_boleta()).grid(row=0, column=4, padx=10, pady=6)
ttk.Button(frame_botones, text="🌳 Aforo por boleta", command=lambda: calcular_aforo()).grid(row=0, column=5, padx=10, pady=6)
ttk.Button(frame_botones, text="🛠️ Herramientas", command=lambda: abrir_herramientas(root, db)).grid(row=0, column=6, padx=10, pady=6)

var_seleccionar_todo = tk.BooleanVar()
ttk.Checkbutton(frame_botones, text="Seleccionar todas", variable=var_seleccionar_todo, command=lambda: toggle_seleccion()).grid(row=0, column=7, padx=10)

# === Tabla ===
columnas_tabla = ["IdMuestra", "Boleta", "Nombre", "Tipo", "Nombre Usuario", "CULTIVO"]
tabla = ttk.Treeview(root, columns=columnas_tabla, show="headings", selectmode="extended")
for col in columnas_tabla:
    tabla.heading(col, text=col)
    tabla.column(col, width=160)
tabla.pack(expand=True, fill="both", padx=10, pady=10)

# === Variables externas ===
resultados_df = pd.DataFrame()

# === Funciones completas ===
def cargar_muestras(usuarios):
    muestras = []
    docs = db.collection("Muestras").stream()
    for doc in docs:
        data = doc.to_dict()
        data["IdMuestra"] = doc.id
        uid = data.get("Usuario", "")
        data["Nombre Usuario"] = usuarios.get(uid, uid)
        data["FechaHora"] = data.get("FechaHora", None)
        muestras.append(data)
    return pd.DataFrame(muestras)

def filtrar():
    df = cargar_muestras(usuarios_dict)
    for campo in ["Boleta", "Nombre", "Tipo", "CULTIVO"]:
        valor = filtros[campo].get()
        if valor:
            df = df[df[campo].astype(str).str.contains(valor, case=False, na=False)]
    valor_usuario = filtros["Usuario"].get()
    if valor_usuario:
        df = df[df["Nombre Usuario"].astype(str).str.contains(valor_usuario, case=False, na=False)]
    if "FechaHora" in df.columns:
        df["FechaHora"] = pd.to_datetime(df["FechaHora"], errors='coerce', utc=True)
        desde = pd.to_datetime(fecha_desde.get_date().strftime("%d/%m/%Y"), utc=True, dayfirst=True)
        hasta = pd.to_datetime(fecha_hasta.get_date().strftime("%d/%m/%Y 23:59:59"), utc=True, dayfirst=True)
        df = df[(df["FechaHora"] >= desde) & (df["FechaHora"] <= hasta)]
    variedad_filtro = filtros["Variedad"].get().strip().lower()
    if variedad_filtro:
        variedad_resultados = []
        for _, row in df.iterrows():
            boleta = str(row.get("Boleta", "")).strip()
            variedad = ""
            if boleta:
                try:
                    doc_eepp = db.collection("EEPP").document(boleta).get()
                    if doc_eepp.exists:
                        variedad = doc_eepp.to_dict().get("Variedad", "")
                except: pass
            if variedad_filtro in variedad.lower():
                row["Variedad"] = variedad
                variedad_resultados.append(row)
        df = pd.DataFrame(variedad_resultados)
    actualizar_tabla(df)

def actualizar_tabla(df):
    for row in tabla.get_children():
        tabla.delete(row)
    for _, row in df.iterrows():
        tabla.insert("", "end", values=[row.get(col, "") for col in columnas_tabla])
    global resultados_df
    resultados_df = df

def toggle_seleccion():
    seleccionar = var_seleccionar_todo.get()
    for item in tabla.get_children():
        tabla.selection_add(item) if seleccionar else tabla.selection_remove(item)

def generar_informe_seleccionado():
    seleccion = tabla.selection()
    if not seleccion:
        messagebox.showwarning("Sin selección", "Debes seleccionar una muestra.")
        return
    if len(seleccion) > 1:
        messagebox.showinfo("Informe individual", "Solo puedes generar un informe detallado a la vez.")
        return
    item = tabla.item(seleccion[0])
    valores = item["values"]
    id_muestra = valores[0]
    cultivo = valores[5]
    uid_usuario = resultados_df[resultados_df["IdMuestra"] == id_muestra]["Usuario"].values[0]
    try:
        generar_pdf(id_muestra, cultivo, uid_usuario)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")

def eliminar_muestras():
    seleccionados = tabla.selection()
    if not seleccionados:
        messagebox.showwarning("Aviso", "No se ha seleccionado ninguna muestra.")
        return
    try:
        docs = db.collection("Borrado").document("Eliminación").get()
        if not docs.exists:
            messagebox.showerror("Error", "No se encontró el código de verificación en Firebase.")
            return
        codigo_correcto = docs.to_dict().get("Valor")
        if not codigo_correcto:
            messagebox.showerror("Error", "El campo 'Valor' no está definido en el documento.")
            return
        codigo_usuario = custom_input_window("Código de verificación", "Introduce el código de eliminación:")

        if codigo_usuario != codigo_correcto:
            messagebox.showerror("Código incorrecto", "El código es incorrecto. No se eliminará nada.")
            return
        confirmacion = messagebox.askyesno("Confirmar eliminación", f"¿Deseas eliminar {len(seleccionados)} muestras?")
        if not confirmacion:
            return
        for item_id in seleccionados:
            muestra_id = tabla.item(item_id, 'values')[0]
            try:
                db.collection("Muestras").document(muestra_id).delete()
                tabla.delete(item_id)
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo eliminar la muestra {muestra_id}.")
    except Exception as e:
        messagebox.showerror("Error", "Ocurrió un error accediendo a Firestore.")

def generar_informe_general():
    seleccionados = tabla.selection()
    if not seleccionados:
        messagebox.showwarning("Aviso", "Debes seleccionar una o más muestras.")
        return
    datos = []
    for item_id in seleccionados:
        valores = tabla.item(item_id)["values"]
        muestra = resultados_df[resultados_df["IdMuestra"] == valores[0]].iloc[0].to_dict()
        datos.append(muestra)
    try:
        generar_pdf_general(datos)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")

def ejecutar_informe_comercial():
    seleccionados = tabla.selection()
    if not seleccionados:
        messagebox.showwarning("Sin selección", "Debes seleccionar una o más muestras.")
        return
    boletas_unicas = set()
    muestras_seleccionadas = []
    for item_id in seleccionados:
        valores = tabla.item(item_id)["values"]
        boleta = valores[1]
        boletas_unicas.add(boleta)
        id_muestra = valores[0]
        muestras_seleccionadas.append(id_muestra)
    popup = tk.Toplevel(root)
    popup.iconphoto(False, logo_icon)

    popup.title("Kg recolectados por boleta")
    entradas_kg = {}
    row = 0
    for boleta in sorted(boletas_unicas):
        tk.Label(popup, text=f"Boleta {boleta} - Kg:").grid(row=row, column=0, padx=10, pady=5)
        entrada = tk.Entry(popup)
        entrada.grid(row=row, column=1, padx=10, pady=5)
        entradas_kg[boleta] = entrada
        row += 1
    def generar():
        try:
            df_filtrado = []
            for id_muestra in muestras_seleccionadas:
                doc = db.collection("Muestras").document(id_muestra).get()
                if doc.exists:
                    data = doc.to_dict()
                    data["IdMuestra"] = id_muestra
                    data["Boleta"] = data.get("Boleta", "")
                    data["CULTIVO"] = data.get("CULTIVO", "")
                    data["Tipo"] = data.get("Tipo", "")
                    data["Nombre"] = data.get("Nombre", "")
                    df_filtrado.append(data)
            lista_datos = df_filtrado
            nombre_referencia = lista_datos[0].get("Nombre") if lista_datos else None
            generar_informe_comercial_desde_ui(lista_datos, nombre=nombre_referencia)
            popup.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")
    tk.Button(popup, text="Generar informe", command=generar).grid(row=row, column=0, columnspan=2, pady=10)
def generar_informe_unico_por_boleta():
    cultivo = filtros["CULTIVO"].get().strip()
    if not cultivo:
        messagebox.showwarning("Filtro requerido", "Debes filtrar un cultivo para usar esta función.")
        return
    if resultados_df.empty:
        messagebox.showinfo("Sin datos", "No hay resultados para generar el informe.")
        return

    df_filtrado = resultados_df.copy()
    df_filtrado = df_filtrado[df_filtrado["CULTIVO"].str.lower() == cultivo.lower()]
    if df_filtrado.empty:
        messagebox.showinfo("Sin datos", f"No se encontraron muestras para el cultivo '{cultivo}'.")
        return
    if "FechaHora" not in df_filtrado.columns:
        messagebox.showerror("Error", "Las muestras no contienen campo 'FechaHora'.")
        return

    try:
        df_filtrado["FechaHora"] = pd.to_datetime(df_filtrado["FechaHora"], errors='coerce', utc=True)
        df_ordenado = df_filtrado.sort_values("FechaHora", ascending=False)
        df_unico = df_ordenado.drop_duplicates(subset="Boleta", keep="first")

        lista_datos = df_unico.to_dict(orient="records")
        generar_pdf_general(lista_datos)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")
def calcular_aforo():
    cultivo = filtros["CULTIVO"].get().strip()
    if not cultivo:
        messagebox.showwarning("Filtro requerido", "Debes filtrar un cultivo para usar esta función.")
        return
    if resultados_df.empty:
        messagebox.showinfo("Sin datos", "No hay muestras para analizar.")
        return

    df_filtrado = resultados_df.copy()
    df_filtrado = df_filtrado[df_filtrado["CULTIVO"].str.lower() == cultivo.lower()]
    if df_filtrado.empty:
        messagebox.showinfo("Sin datos", f"No se encontraron muestras para el cultivo '{cultivo}'.")
        return

    if "FechaHora" not in df_filtrado.columns or "Boleta" not in df_filtrado.columns:
        messagebox.showerror("Error", "Faltan campos necesarios en los datos.")
        return

    try:
        df_filtrado["FechaHora"] = pd.to_datetime(df_filtrado["FechaHora"], errors='coerce', utc=True)
        df_ordenado = df_filtrado.sort_values("FechaHora", ascending=False)
        df_unico = df_ordenado.drop_duplicates(subset="Boleta", keep="first")

        resultados = []
        for _, row in df_unico.iterrows():
            boleta = str(row.get("Boleta", "")).strip()
            aforo_valor = str(row.get("Aforo (Kg/árbol)", "")).strip()
            try:
                aforo = float(aforo_valor.replace(",", "."))
                if aforo < 0:
                    aforo = 0
            except:
                aforo = 0

            try:
                doc_eepp = db.collection("EEPP").document(boleta).get()
                if not doc_eepp.exists:
                    continue
                datos = doc_eepp.to_dict()
                arboles_valor = str(datos.get("Arbol", "")).strip()
                try:
                    arboles = float(arboles_valor.replace(",", "."))
                    if arboles <= 0:
                        continue
                except:
                    continue

                total_kg = round(aforo * arboles, 2)
                resultados.append((boleta, aforo, arboles, total_kg))
            except:
                continue

        if not resultados:
            messagebox.showinfo("Sin resultados", "No se encontraron datos válidos de aforo.")
            return

        mostrar_resultados_aforo(resultados)

    except Exception as e:
        messagebox.showerror("Error", f"Ocurrió un error:\n{str(e)}")
def mostrar_resultados_aforo(resultados):
    ventana = tk.Toplevel(root)
    ventana.title("Resultado Aforo por Boleta")
    if logo_icon:
        ventana.iconphoto(False, logo_icon)

    ttk.Label(ventana, text="Resumen de Aforo (Kg estimados por boleta):", font=("Segoe UI", 11, "bold")).pack(pady=(10,5))

    tabla = ttk.Treeview(ventana, columns=["Boleta", "Kg/Árbol", "Árboles", "Total Kg"], show="headings")
    for col in ["Boleta", "Kg/Árbol", "Árboles", "Total Kg"]:
        tabla.heading(col, text=col)
        tabla.column(col, anchor="center", width=120)
    tabla.pack(expand=True, fill="both", padx=10, pady=10)

    for boleta, aforo, arboles, total in resultados:
        tabla.insert("", "end", values=[boleta, aforo, arboles, total])

    ttk.Button(ventana, text="Cerrar", command=ventana.destroy).pack(pady=(0, 10))

# Ejecutar app
root.mainloop()
