# =========================================
# STEP 1: IMPORT LIBRARIES
# =========================================

from flask import Flask, jsonify, request
import oracledb

# =========================================
# STEP 2: CREATE FLASK APP
# =========================================

app = Flask(__name__)

# =========================================
# STEP 3: ORACLE CONNECTION FUNCTION
# =========================================

def get_connection():
    """
    Connects Python to Oracle DB (on-prem FX table source)
    """
    conn = oracledb.connect(
        user="spadfdb",
        password="spadfdb",
        dsn="localhost:1521/orclpdb"
    )
    return conn


# =========================================
# STEP 4: GET API (READ FROM ORACLE)
# =========================================

@app.route("/fx", methods=["GET"])
def get_fx():
    """
    Fetch FX_TABLE data from Oracle DB
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT currency, rate FROM spadfdb.FX_TABLE")

    rows = cursor.fetchall()

    result = []

    for r in rows:
        result.append({
            "currency": r[0],
            "rate": r[1]
        })

    conn.close()

    return jsonify(result)


# =========================================
# STEP 5: POST API (INSERT INTO ORACLE)
# =========================================

@app.route("/fx", methods=["POST"])
def insert_fx():
    """
    Insert new record into FX_TABLE
    """

    data = request.json

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO FX_TABLE (currency, rate) VALUES (:1, :2)",
        (data["currency"], data["rate"])
    )

    conn.commit()
    conn.close()

    return jsonify({"message": "Inserted successfully"})


# =========================================
# STEP 6: PUT API (UPDATE ORACLE TABLE)
# =========================================

@app.route("/fx", methods=["PUT"])
def update_fx():
    """
    Update FX rate in Oracle table
    """

    data = request.json

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE FX_TABLE SET rate = :1 WHERE currency = :2",
        (data["rate"], data["currency"])
    )

    conn.commit()
    conn.close()

    return jsonify({"message": "Updated successfully"})


# =========================================
# STEP 7: RUN APPLICATION
# =========================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)