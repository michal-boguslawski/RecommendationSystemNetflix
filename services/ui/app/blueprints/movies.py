from flask import Blueprint, render_template, redirect, url_for


movies_bp = Blueprint(
    "movies",
    __name__,
    template_folder="../templates",
    static_folder="../static",
    url_prefix="/movies",
)


@movies_bp.route("/recommend", methods=["GET"])
def recommend():
    return render_template("recommendation.html")
