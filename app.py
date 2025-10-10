# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])

    @app.get("/test_db", endpoint="test_db")
    
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1, coords2):
        try:
            # Parse the coordinates
            x1, y1, z1 = map(int, coords1.split("_"))
            x2, y2, z2 = map(int, coords2.split("_"))

            eng = get_engine()
            with eng.begin() as conn:
                # Query for studies mentioning the first set of coordinates
                query1 = text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x1 AND ST_Y(geom) = :y1 AND ST_Z(geom) = :z1
                """)
                studies1 = {row["study_id"] for row in conn.execute(query1, {"x1": x1, "y1": y1, "z1": z1}).mappings()}

                # Query for studies mentioning the second set of coordinates
                query2 = text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x2 AND ST_Y(geom) = :y2 AND ST_Z(geom) = :z2
                """)
                studies2 = {row["study_id"] for row in conn.execute(query2, {"x2": x2, "y2": y2, "z2": z2}).mappings()}

            # Find studies that mention the first set but not the second
            a_to_b = list(studies1 - studies2)

            # Find studies that mention the second set but not the first
            b_to_a = list(studies2 - studies1)

            # Return both directions in one response
            return jsonify({
                "a_to_b": a_to_b,  # Studies mentioning coords1 but not coords2
                "b_to_a": b_to_a   # Studies mentioning coords2 but not coords1
            }), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a, term_b):
        # 自動將底線換成空格
        term_a = term_a.replace("_", " ")
        term_b = term_b.replace("_", " ")
        try:
            eng = get_engine()
            with eng.begin() as conn:
                # 確保使用正確的 schema
                conn.execute(text("SET search_path TO ns, public;"))
                # 使用 LIKE 查詢，模糊比對 term
                query = text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term LIKE :term_a
                      AND study_id NOT IN (
                          SELECT study_id
                          FROM ns.annotations_terms
                          WHERE term LIKE :term_b
                      );
                """)
                studies = [row["study_id"] for row in conn.execute(
                    query,
                    {"term_a": f"%{term_a}%", "term_b": f"%{term_b}%"
                }).mappings()]
            # 返回結果，包含 study_count 欄位
            return jsonify({
                "study_count": len(studies),
                "dissociated_studies": studies
            }), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/search_term/<term>", endpoint="search_term")
    def search_term(term):
        eng = get_engine()
        result = {
            "term": term,
            "exists": False,
            "matches": []
        }
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                # 查詢 term 是否存在
                rows = conn.execute(
                    text("SELECT DISTINCT term FROM ns.annotations_terms WHERE term ILIKE :term LIMIT 10"),
                    {"term": f"%{term}%"
                }).mappings().all()
                result["matches"] = [row["term"] for row in rows]
                result["exists"] = len(result["matches"]) > 0
            return jsonify(result), 200
        except Exception as e:
            result["error"] = str(e)
            return jsonify(result), 500

    @app.get("/terms/<term>/count", endpoint="term_count")
    def term_count(term):
        # 自動將底線換成空格
        term = term.replace("_", " ")
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                query = text("""
                    SELECT COUNT(DISTINCT study_id) AS study_count
                    FROM ns.annotations_terms
                    WHERE term LIKE :term
                """)
                result = conn.execute(query, {"term": f"%{term}%"}).mappings().first()
                count = result["study_count"] if result else 0
            return jsonify({
                "term": term,
                "study_count": count
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/terms/<term_a>/<term_b>/intersection_count", endpoint="intersection_count")
    def intersection_count(term_a, term_b):
        # 將底線換成空格
        term_a = term_a.replace("_", " ")
        term_b = term_b.replace("_", " ")
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                query = text("""
                    SELECT COUNT(*) AS intersection_count
                    FROM (
                        SELECT study_id
                        FROM ns.annotations_terms
                        WHERE term LIKE :term_a
                        INTERSECT
                        SELECT study_id
                        FROM ns.annotations_terms
                        WHERE term LIKE :term_b
                    ) AS intersected
                """)
                result = conn.execute(query, {
                    "term_a": f"%{term_a}%",
                    "term_b": f"%{term_b}%"
                }).mappings().first()
                count = result["intersection_count"] if result else 0
            return jsonify({
                "intersection_count": count,
                "term_a": term_a,
                "term_b": term_b
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return app

# WSGI entry point (no __main__)
app = create_app()

# 啟動 Web 伺服器
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
