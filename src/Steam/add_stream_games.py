import pandas as pd
import requests
# Lista de nombres nuevos que quieres verificar
nuevos_juegos = ["Counter-Strike 2", "Un juego nuevo", "Stardew Valley"] #ENTRADA DE DATOS STREAM

# Cargar CSV existente con columnas 'Game Name' y 'AppID'
csv_path = "steam_games.csv"
df = pd.read_csv(csv_path)

# Convertir nombres existentes en un set para búsqueda rápida
juegos_existentes = set(df["Game Name"].str.lower())


def obtener_appid(nombre_juego):
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    respuesta = requests.get(url)

    if respuesta.status_code != 200:
        print("Error al obtener la lista de juegos.")
        return None

    juegos = respuesta.json()['applist']['apps']
    for juego in juegos:
        if juego['name'].lower() == nombre_juego.lower():
            return juego['appid']
    
    print("Juego no encontrado.")
    return None



nuevas_filas = []
for juego in nuevos_juegos:
    if juego.lower() not in juegos_existentes:
        appid = obtener_appid(juego)
        if appid != None:
            nuevas_filas.append({"Game Name": juego, "AppID": appid})


if nuevas_filas:
    df_nuevos = pd.DataFrame(nuevas_filas)
    df = pd.concat([df, df_nuevos], ignore_index=True)
    df.to_csv(csv_path, index=False)
    print(f"CSV actualizado con {len(nuevas_filas)} nuevo(s) juego(s).")
else:
    print("Todos los juegos ya están presentes en el CSV.")
