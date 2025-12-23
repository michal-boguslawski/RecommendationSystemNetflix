import { loadRecommendations, loadUsers, loadRatedMoviesAtStart, loadMovies } from "./data_loader.js";
import { getAllFromUrl } from "./url_helpers.js";


// on start
document.addEventListener("DOMContentLoaded", () => {
    const params = getAllFromUrl()
    console.log("Params: ", params)

    loadUsers(params).then(() => {
        loadRatedMoviesAtStart(params);
    })

    loadMovies(params);

    if (params.show_recs) {
        loadRecommendations(params);
    }
});
