import os
import json
import boto3
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify

app = Flask(__name__)

# Logging
logging.basicConfig(level=logging.INFO)

# X-Ray tracing

# Config from environment
REGION = os.environ.get("AWS_REGION", "ap-south-1")
COURSE_URL = os.environ.get("COURSE_SERVICE_URL", "http://course-service")
TABLE_NAME = os.environ.get("STUDENT_TABLE", "Anirudh-StudentTable")

# DynamoDB (IRSA credentials assumed)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
students_table = dynamodb.Table(TABLE_NAME)

# HTTP session with retry
session = requests.Session()
retry = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[502, 503, 504],
)
session.mount("http://", HTTPAdapter(max_retries=retry))


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "student-service",
        "region": REGION
    }), 200


@app.route("/students/<student_id>", methods=["GET"])
def get_student(student_id):
    try:
        resp = students_table.get_item(Key={"id": student_id})
    except Exception as e:
        logging.error(f"DynamoDB error: {e}")
        return jsonify({"error": "Database error"}), 500

    item = resp.get("Item")

    if not item:
        return jsonify({"error": "Student not found"}), 404

    # Enrich with course data
    course_code = item.get("course-id")

    if course_code:
        try:
            r = session.get(f"{COURSE_URL}/courses/{course_code}", timeout=2)

            if r.status_code == 200:
                item["course"] = r.json()
            else:
                item["course"] = {"code": course_code, "title": None}

        except requests.RequestException as e:
            logging.warning(f"Course service error: {e}")
            item["course"] = {"code": course_code, "title": None}

    return jsonify(item), 200


@app.route("/students", methods=["GET"])
def list_students():
    try:
        resp = students_table.scan(Limit=50)
        items = resp.get("Items", [])
    except Exception as e:
        logging.error(f"DynamoDB scan error: {e}")
        return jsonify({"error": "Database error"}), 500

    return jsonify(items), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)