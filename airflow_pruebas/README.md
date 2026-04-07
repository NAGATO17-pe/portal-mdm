# Entorno de Pruebas: Apache Airflow

Esta carpeta contiene todo lo necesario para levantar un entorno de pruebas ligero de **Apache Airflow** en tu máquina local. Debido a que Airflow no tiene soporte oficial nativo en Windows, la mejor manera y la más recomendada de usarlo (incluso para pruebas) es mediante **Docker**.

## 📁 Estructura del directorio

- `docker-compose.yaml`: Configuración para levantar un contenedor `standalone` de Airflow (incluye servidor web, scheduler y base de datos SQLite en un solo lugar).
- `dags/`: Carpeta donde debes poner todos tus archivos `.py` que contengan tus flujos de trabajo (DAGs). Ya incluye un DAG de ejemplo (`test_dag.py`).
- `logs/`: (Se generará automáticamente). Aquí se guardarán los logs de ejecución de las tareas.
- `plugins/`: (Opcional). Aquí puedes incluir componentes personalizados de Airflow.

## 🚀 Cómo ejecutarlo

### Requisitos Previos
1. Necesitas tener instalado **Docker Desktop** en tu Windows. Si no lo tienes, puedes descargarlo de: [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
2. Asegúrate de tener Docker Desktop abierto y corriendo.

### Pasos para levantar Airflow

1. Abre una terminal (PowerShell o CMD) y navega a esta carpeta:
   ```bash
   cd "D:\Proyecto2026\ACP_DWH\airflow_pruebas"
   ```

2. Ejecuta el siguiente comando para descargar la imagen y levantar el servidor de pruebas:
   ```bash
   docker-compose up -d
   ```

3. **Acceder a la Interfaz Web:**
   Una vez que termine de cargar, abre tu navegador web y ve a:
   [http://localhost:8080](http://localhost:8080)

   - **Usuario:** `admin`
   - **Contraseña:** Para encontrar la contraseña autogenerada en el modo *standalone*, debes revisar los logs de Docker. Ejecuta en la terminal:
     ```bash
     docker logs airflow_test_standalone | findstr "password"
     ```
     *(La contraseña aparecerá en la terminal como parte del texto de inicio).*

4. **Detener el servidor:**
   Para detener y apagar el entorno de pruebas sin borrar tus datos, ejecuta:
   ```bash
   docker-compose stop
   ```
   *Nota:* Para eliminar el contenedor completamente puedes usar `docker-compose down`.

## ✍️ Creando más pruebas

Para crear nuevos procesos y validaciones, tan solo debes crear o copiar archivos `.py` dentro de la carpeta `dags/`. Airflow escaneará la carpeta automáticamente cada cierto tiempo (suele tardar un par de minutos) y verás aparecer tus nuevos DAGs en la interfaz web de localhost:8080.
