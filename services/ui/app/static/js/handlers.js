import { setInUrl, removeFromUrl, getAllFromUrl } from "./url_helpers.js";
import { clearContainer } from "./dom_helpers.js";
import { renderSelection } from "./render_shared.js";
import { loadUsers, loadMovies, loadRatedMovies, loadRecommendations } from "./data_loader.js";


const function_to_call = {
    "users": loadUsers,
    "movies": loadMovies,
    "rated_movies": loadRatedMovies,
    "recommendations": loadRecommendations,
}


export function clickUsers(userId) {
    const params = getAllFromUrl()
    loadRatedMovies({...params, users: userId});
    renderSelection(userId, "users", true);

    clearContainer("recommendations")
    clearContainer("recommendations_pagination")

    setInUrl("users", userId);
    removeFromUrl("rated_movies_page")
    removeFromUrl("rated_movies")
    removeFromUrl("recommendations_page")
    removeFromUrl("recommendations")
    removeFromUrl("show_recs")
}


export function clickMovie(type, movieId) {
    // Get current movies from URL
    const params = getAllFromUrl()
    console.log("Params movies: ", params.movies)
    const moviesArray = Array.isArray(params.movies)
        ? params.movies
        : params.movies != null
            ? [params.movies]   // single value
            : [];               // no movies

    // Toggle movieId
    let distinctMovies;
    if (moviesArray.includes(movieId)) {
        // Remove it
        distinctMovies = moviesArray.filter(m => m !== movieId);
    } else {
        // Add it
        distinctMovies = [...moviesArray, movieId];
    }

    console.log("Distinct movies:", distinctMovies);

    loadUsers({...params, movies: distinctMovies})

    // Update UI
    renderSelection(distinctMovies, type, true);

    // Update URL
    setInUrl(type, distinctMovies);
}


export function clickPagination(type, page) {
    const params = getAllFromUrl();
    params[`${type}_page`] = page;
    function_to_call[type](params)
    setInUrl(`${type}_page`, page)
}


// add listener to recommendation button
const button = document.getElementById("get_recommendations");

button.addEventListener("click", () => {
    const params = getAllFromUrl();
    if (!params.users) {
        alert("Please select a user first");
        return;
    }
    loadRecommendations(params)
    setInUrl("show_recs", true)
});
