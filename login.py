from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, JWTManager
from datetime import timedelta
import cx_Oracle
import os
from dotenv import load_dotenv
from flask_jwt_extended import get_jwt
import pymysql


# ------------------- Configuraciones y env ----------------------------

# Cargar las variables de entorno
load_dotenv()

# Obtener credenciales de las variables de entorno
DB_USER = os.getenv("DB_NET_USER")
DB_PASSWORD = os.getenv("DB_NET_PASSWORD")
DB_HOST = os.getenv("DB_NET_HOST")
DB_PORT = os.getenv("DB_NET_PORT")
DB_NAME = os.getenv("DB_NET_NAME")

DB_PRENDAS_USER = os.getenv("DB_PRENDAS_USER")
DB_PRENDAS_PASSWORD = os.getenv("DB_PRENDAS_PASSWORD")
DB_PRENDAS_HOST = os.getenv("DB_PRENDAS_HOST")
DB_PRENDAS_PORT = int(os.getenv("DB_PRENDAS_PORT"))
DB_PRENDAS_NAME = os.getenv("DB_PRENDAS_NAME")

db_config = {
    'host': DB_PRENDAS_HOST,
    'port': DB_PRENDAS_PORT,
    'user': DB_PRENDAS_USER,
    'password': DB_PRENDAS_PASSWORD,
    'database': DB_PRENDAS_NAME,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}

DASHBOARD_URL = "http://128.0.16.240:5013"
dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, sid=DB_NAME)
connection = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)

app = Flask(__name__)

# Configuración del JWT
app.config["JWT_SECRET_KEY"] = "supersecretkey"  # Cambia esto por una clave segura
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=12)  # El token expira en 1 hora

jwt = JWTManager(app)

# ----------------------------------------------------------------------

# ------------------------------ Funciones -----------------------------

def verify_user(user: str, pwd: str):
    try:
        cursor = connection.cursor()
        username = cursor.var(cx_Oracle.STRING)
        p_menserro = cursor.var(cx_Oracle.STRING)
        cursor.callproc("prc_login",[user, pwd, username, p_menserro])
        if p_menserro.getvalue():
            return p_menserro.getvalue()
        else:
            return username.getvalue(), "Verificacion Correcta"

    except Exception as e:
        print(e)
    finally:
        cursor.close()

# ----------------------------------------------------------------------

# -------------------------------- Flask -------------------------------

# Ruta para renderizar el login
@app.route("/")
def login_page():
    return render_template("login.html")

# Ruta para autenticación
@app.route("/login", methods=["POST"])
def login():
    data = request.json
    usercode = data.get("username")
    password = data.get("password")

    username, verify = verify_user(usercode, password)

    if verify == "Verificacion Correcta":
        additional_claims = {"username": username}
        access_token = create_access_token(identity=usercode, additional_claims=additional_claims)
        return jsonify({"access_token": access_token, "redirect_url": DASHBOARD_URL}), 200
    else:
        return jsonify({"message": "Estamos trabajando, un momento por favor..."}), 401

# Ruta protegida con autenticación
@app.route("/protected", methods=["GET"])
@jwt_required()
def protected():
    current_user = get_jwt_identity()
    username = get_jwt()["username"]
    return jsonify({"usercode": current_user, "username": username}), 200

@app.route('/save_response', methods=['POST'])
def save_response():
    try:
        data = request.json
        codigo_usuario = data.get("usercode")
        nombre_usuario = data.get("username")
        ol = data.get("ol")
        historial = data.get("history")

        if not all([codigo_usuario, nombre_usuario, ol, historial]):
            return jsonify({"error": "Faltan datos"}), 400

        # Conectar a la base de datos
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Insertar los datos en la tabla de forma segura
        query = """
        INSERT INTO prdohistmatz (TIDENCODE, TIDENUSUA, TNUMEROOL, THISTCONS)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (codigo_usuario, nombre_usuario, ol, historial))
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({"mensaje": "Respuesta guardada exitosamente"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port='5021')
