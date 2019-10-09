import json
import uuid
from functools import wraps

import flask as flask
from flask import request, abort


def load_or_create_config():
    try:
        with open("config-server.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        key = str(uuid.uuid4())
        conf = {
            "AUTH_KEY": key,
            "PORT": 17985
        }
        with open("config-server.json", "w") as f:
            json.dump(conf, f)
        return conf


def auth_required(f):
    @wraps(f)
    def decorated_func(*args, **kwargs):
        if not CONFIG.get("AUTH_KEY"):
            abort(401)
        if 'Authorization' not in request.headers:
            abort(401)
        if request.headers['Authorization'] != CONFIG["AUTH_KEY"]:
            abort(401)
        return f(*args, **kwargs)
    return decorated_func


CONFIG = load_or_create_config()
app = flask.Flask(__name__)


@auth_required
@app.route("/", defaults={'path': ''})
@app.route("/<path:path>")
def catch_all_get(path):
    with open(path, "r") as f:
        return f.read()


@auth_required
@app.route('/', defaults={'path': ''}, methods=['POST'])
@app.route('/<path:path>', methods=['POST'])
def catch_all_post(path):
    with open(path, "w") as f:
        if request.json:
            json.dump(request.json, f)
            return f"Saved {path}"
        abort(401)


if __name__ == '__main__':
    app.run(port=CONFIG['PORT'])
