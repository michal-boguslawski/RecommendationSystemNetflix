from flask import Flask
from .blueprints.movies import movies_bp
from .blueprints.home import home_bp


def create_app():
    app = Flask(__name__)
    app.secret_key = "super-secret"

    # Register blueprint
    app.register_blueprint(movies_bp)
    app.register_blueprint(home_bp)
    return app