# === HARVESTSYNC DESK IMPLEMENTADO Y LISTO PARA DISTRIBUCIÓN ===
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import webbrowser
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
from informe_generator import generar_pdf
from informe_generator_general import generar_pdf_general
from informe_generator_comercial import generar_informe_comercial_desde_ui
import sys, os
import traceback
from tkinter.simpledialog import askstring

print("🔹 Script iniciado")  # Justo después del import sys, os








def recurso_path(rel_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, rel_path)
    return os.path.join(os.path.abspath("."), rel_path)

# === INICIALIZAR FIREBASE ===
cred = credentials.Certificate(recurso_path("HarvestSync.json"))
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()
# Tras inicializar Firebase
print("✅ Firebase inicializado correctamente")
# === FUNCIONES ===
def cargar_usuarios():
    usuarios = {}
    docs = db.collection("UsuariosAutorizados").stream()
    for doc in docs:
        data = doc.to_dict()
        usuarios[doc.id] = data.get("Nombre", doc.id)
    return usuarios

usuarios_dict = cargar_usuarios()

def cargar_muestras(usuarios):
    muestras = []
    docs = db.collection("Muestras").stream()
    for doc in docs:
        data = doc.to_dict()
        data["IdMuestra"] = doc.id
        uid = data.get("Usuario", "")
        data["NombreUsuario"] = usuarios.get(uid, uid)
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
        df = df[df["NombreUsuario"].astype(str).str.contains(valor_usuario, case=False, na=False)]

    if "FechaHora" in df.columns:
        df["FechaHora"] = pd.to_datetime(df["FechaHora"], errors='coerce', utc=True)
        desde = pd.to_datetime(fecha_desde.get_date().strftime("%d/%m/%Y"), utc=True, dayfirst=True)
        hasta = pd.to_datetime(fecha_hasta.get_date().strftime("%d/%m/%Y 23:59:59"), utc=True, dayfirst=True)
        df = df[(df["FechaHora"] >= desde) & (df["FechaHora"] <= hasta)]

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
        buffer = generar_pdf(id_muestra, cultivo, uid_usuario)
        with open("informe.pdf", "wb") as f:
            f.write(buffer.read())
        webbrowser.open("informe.pdf")
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")

def eliminar_muestras():
    seleccionados = tabla.selection()
    if not seleccionados:
        messagebox.showwarning("Aviso", "No se ha seleccionado ninguna muestra.")
        return

    try:
        # Consultar el valor de verificación en Firestore
        docs = db.collection("Borrado").document("Eliminación").get()
        if not docs.exists:
            messagebox.showerror("Error", "No se encontró el código de verificación en Firebase.")
            return

        codigo_correcto = docs.to_dict().get("Valor")
        if not codigo_correcto:
            messagebox.showerror("Error", "El campo 'Valor' no está definido en el documento.")
            return

        # Solicitar código al usuario
        codigo_usuario = askstring("Código de verificación", "Introduce el código de eliminación:")
        if codigo_usuario != codigo_correcto:
            messagebox.showerror("Código incorrecto", "El código de verificación es incorrecto. No se eliminará ninguna muestra.")
            return

        # Confirmación final
        confirmacion = messagebox.askyesno("Confirmar eliminación", f"¿Deseas eliminar {len(seleccionados)} muestras?")
        if not confirmacion:
            return

        for item_id in seleccionados:
            muestra_id = tabla.item(item_id, 'values')[0]
            try:
                db.collection("Muestras").document(muestra_id).delete()
                tabla.delete(item_id)
            except Exception as e:
                print(f"Error eliminando {muestra_id}: {e}")
                messagebox.showerror("Error", f"No se pudo eliminar la muestra {muestra_id}.")

    except Exception as e:
        print("Error accediendo a Firestore:", e)
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
        buffer = generar_pdf_general(datos)
        with open("informe_general.pdf", "wb") as f:
            f.write(buffer.read())
        webbrowser.open("informe_general.pdf")
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
            print("🔄 Generando informe comercial...")
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
            print("📄 Muestras cargadas desde Firestore:")
            for m in lista_datos:
                print(m["IdMuestra"], m["Boleta"], m["CULTIVO"], m["Tipo"], m["Nombre"])

            generar_informe_comercial_desde_ui(lista_datos)
            print("✅ PDF generado correctamente.")
            webbrowser.open("informe_comercial.pdf")
            popup.destroy()
        except Exception as e:
            print("❌ Error al generar informe:", e)
            messagebox.showerror("Error", f"No se pudo generar el informe:\n{str(e)}")

    tk.Button(popup, text="Generar informe", command=generar).grid(row=row, column=0, columnspan=2, pady=10)

# === INTERFAZ ===
try:
    # Antes de crear la ventana
    print("🪟 Creando ventana principal...") 
    root = tk.Tk()
     # Justo después de root = tk.Tk()
    print("🎯 root creado")

    root.title("HarvestSync Desk")
    root.geometry("1200x650")
    try:
        root.iconbitmap(recurso_path("icono_app.ico"))
    except Exception as e:
        print(f"❌ No se pudo cargar el icono: {e}")


    frame_filtros = tk.Frame(root)
    frame_filtros.pack(pady=10)

    filtros = {}
    campos = ["Boleta", "Nombre", "Tipo", "Usuario", "CULTIVO"]
    for idx, campo in enumerate(campos):
        tk.Label(frame_filtros, text=campo).grid(row=0, column=idx)
        entry = tk.Entry(frame_filtros)
        entry.grid(row=1, column=idx)
        filtros[campo] = entry

    tk.Label(frame_filtros, text="Desde").grid(row=0, column=len(campos))
    fecha_desde = DateEntry(frame_filtros, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern="dd/mm/yyyy")
    fecha_desde.grid(row=1, column=len(campos))

    tk.Label(frame_filtros, text="Hasta").grid(row=0, column=len(campos)+1)
    fecha_hasta = DateEntry(frame_filtros, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern="dd/mm/yyyy")
    fecha_hasta.grid(row=1, column=len(campos)+1)

    tk.Button(root, text="Buscar muestras", command=filtrar).pack(pady=5)
    tk.Button(root, text="Generar informe de muestra seleccionada", command=generar_informe_seleccionado).pack(pady=5)
    tk.Button(root, text="🗑️ Eliminar seleccionados", command=eliminar_muestras).pack(pady=5)
    tk.Button(root, text="📦 Generar informe comercial", command=ejecutar_informe_comercial).pack(pady=5)
    tk.Button(root, text="📊 Generar informe general", command=generar_informe_general).pack(pady=5)

    toggle_frame = tk.Frame(root)
    toggle_frame.pack(pady=5)
    var_seleccionar_todo = tk.BooleanVar()
    tk.Checkbutton(toggle_frame, text="Seleccionar todas", variable=var_seleccionar_todo, command=toggle_seleccion).pack()

    columnas_tabla = ["IdMuestra", "Boleta", "Nombre", "Tipo", "NombreUsuario", "CULTIVO"]
    tabla = ttk.Treeview(root, columns=columnas_tabla, show="headings", selectmode="extended")
    for col in columnas_tabla:
        tabla.heading(col, text=col)
        tabla.column(col, width=150)
    tabla.pack(expand=True, fill="both")

    resultados_df = pd.DataFrame()
    # Justo antes de root.mainloop()
    print("🚀 Ejecutando mainloop()")
    root.mainloop()
except Exception as e:
    import traceback
    with open("error_log.txt", "w") as f:
        f.write(traceback.format_exc())
    raise  # Para que se muestre también por consola
