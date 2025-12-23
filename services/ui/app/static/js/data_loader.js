import { call_api } from "./api.js"
import { renderDict, renderList, set_pagination } from "./render.js";

        
export async function loadUsers({
    users=null,
    users_page=1,
    users_page_size=20,
    movies=null
}) {
    const type = "users"
    const api_params = {
        type: type,
        page: users_page,
        page_size: users_page_size,
        movies: movies
    }

    console.log("Users API call params: ", api_params)

    const data = await call_api(api_params);

    renderList(data, type, users);

    set_pagination(type, users_page, data.total_pages, data.has_next, data.has_prev);

    // renderSelection(userId, type);
}

export async function loadMovies({
    movies_page=1, movies_page_size=20, movies=null
}) {
    const type = "movies"
    const api_params = {
        type: type,
        page: movies_page,
        page_size: movies_page_size,
    }
    
    console.log("Movies API call params: ", api_params)

    const data = await call_api(api_params);

    renderDict(data, type, movies);

    set_pagination(type, movies_page, data.total_pages, data.has_next, data.has_prev);
}

export async function loadRatedMovies({
    users,
    rated_movies_page=1,
    rated_movies_page_size=20,
    rated_movies=null
}) {
    const type = "rated_movies"
    const api_params = {
        type: type,
        id: users,
        page: rated_movies_page,
        page_size: rated_movies_page_size,
    }

    const data = await call_api(api_params);

    renderDict(data, type, rated_movies);

    set_pagination(type, rated_movies_page, data.total_pages, data.has_next, data.has_prev);
}

export function loadRatedMoviesAtStart(params = {}) {
    if (params.users) {
        loadRatedMovies(params);
    }
}

export async function loadRecommendations({
    users,
    recommendations_page=1,
    recommendations_page_size=20,
    recommendations=null
}) {
    
    const type = "recommendations";
    const api_params = {
        type: type,
        id: users,
        page: recommendations_page,
        page_size: recommendations_page_size,
    }
    const data = await call_api(api_params);

    renderDict(data, type, recommendations);

    set_pagination(type, recommendations_page, data.total_pages, data.has_next, data.has_prev);
}
