import json
import urllib.request
import time

URL_BASE = "http://127.0.0.1:8000"

def simular_nextjs():
    print("=" * 60)
    print("🚀 SIMULADOR DE FRONTEND NEXT.JS (Cliente en Python) 🚀")
    print("=" * 60)
    
    # 1. El usuario hace click en "Iniciar ETL" en el Frontend
    print("[1] El usuario hizo click en 'Ejecutar Corrida'...")
    datos_post = json.dumps({"iniciado_por": "Analista Senior (Simulador)"}).encode("utf-8")
    
    req_post = urllib.request.Request(
        f"{URL_BASE}/api/etl/corridas",
        data=datos_post,
        headers={"Content-Type": "application/json"}
    )
    
    try:
        with urllib.request.urlopen(req_post) as respuesta:
            cuerpo = json.loads(respuesta.read().decode("utf-8"))
            
        print(f"✅ Backend aceptó la corrida.")
        print(f"   ID Corrida: {cuerpo['id_corrida']}")
        print(f"   ID Log en BD: {cuerpo['id_log']}")
        print("-" * 60)
        
        # 2. El Frontend se conecta al WebSocket/SSE para leer la consola remota
        url_sse = f"{URL_BASE}{cuerpo['url_stream']}"
        print(f"[2] Frontend conectándose al canal de tiempo real para escuchar eventos...\n")
        
        req_sse = urllib.request.Request(url_sse)
        
        with urllib.request.urlopen(req_sse) as stream:
            for linea_cruda in stream:
                linea = linea_cruda.decode("utf-8").strip()
                
                # Omitir líneas vacías del protocolo SSE
                if not linea:
                    continue
                    
                # SSE envía los datos con el prefijo "data: "
                if linea.startswith("data: "):
                    contenido = linea[6:] # Quitamos "data: "
                    
                    if contenido == "[FIN_CORRIDA]":
                        print("\n[🔌 NEXT.JS] Canal SSE cerrado limpiamente. Evento terminado.")
                        break
                        
                    # Simulamos una interfaz gráfica imprimiendo la consola
                    print(f"🖥️  {contenido}")
                    
    except Exception as e:
        print(f"❌ Error conectando al Backend: {e}")
        print("Asegúrate de que 'uvicorn main:aplicacion' esté corriendo en otra terminal.")

if __name__ == "__main__":
    simular_nextjs()
