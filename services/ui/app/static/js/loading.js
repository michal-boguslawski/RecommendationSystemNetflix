import { call_api } from "./api_functions.js"
import { setInUrl, getFromUrl, removeFromUrl, clearContainer } from "./helper_functions.js"

const function_to_call = {
    "users": loadUsers,
    "movies": loadMovies,
    "rated_movies": loadRatedMovies,
    "recommendations": loadRecommendations,
}

document.addEventListener("DOMContentLoaded", () => {
    const userId = Number(getFromUrl("user", null))
    const users_page = Number(getFromUrl("users_page", 1));
    loadUsers(userId, users_page).then(() => {
        loadRatedMoviesAtStart();
    })

    const movies_page = Number(getFromUrl("movies_page", 1));
    loadMovies(null, movies_page);

    const recommendations_page = Number(getFromUrl("recommendations_page", 1));
    const show_recs = getFromUrl("show_recs", false);
    if (show_recs) {
        loadRecommendations(userId, recommendations_page);
    }
});
        
async function loadUsers(userId, page=1, pageSize=20, movies) {
    const type = "users"
    const data = await call_api(type, null, page, pageSize, movies);

    renderList(data, type);

    set_pagination(type, page, data.total_pages, data.has_next, data.has_prev);

    renderSelection(userId, type);
}

async function loadMovies(undefined, page=1, pageSize=20) {
    const type = "movies"
    const data = await call_api(type, null, page, pageSize, null);

    renderDict(data, type);

    set_pagination(type, page, data.total_pages, data.has_next, data.has_prev);
}

async function loadRatedMovies(userId, page=1, pageSize=20, desc=true) {
    const type = "rated_movies"
    const data = await call_api(type, userId, page, pageSize, null);

    renderDict(data, type, "enumerated");

    set_pagination(type, page, data.total_pages, data.has_next, data.has_prev);
}

function loadRatedMoviesAtStart() {
    const userId = Number(getFromUrl("user", null));
    const rated_movies_page = Number(getFromUrl("rated_movies_page", 1));
    if (userId) {
        loadRatedMovies(userId, rated_movies_page);
        renderSelection(userId, "users");
    }
}

export async function clickUsers(userId) {

    loadRatedMovies(userId);
    renderSelection(userId, "users", true);

    clearContainer("recommendations")

    setInUrl("user", userId);
    removeFromUrl("rated_movies_page")
    removeFromUrl("recommendations_page")
    removeFromUrl("show_recs")
}

export function renderList(data, element_id) {
    const container = document.getElementById(element_id);
    container.innerHTML = "";

    // adjust based on API response shape

    for (const userId of data.users ?? []) {
        const button = document.createElement("button");

        button.textContent = userId;
        button.dataset.id = userId;
        button.className = "option";
        button.id = `${element_id}_${userId}`;

        button.addEventListener("click", () => {
            console.log("Clicked ", element_id,": " , userId);
            clickUsers(userId);
        })

        container.appendChild(button);
    }
}

export function renderDict(data, element_id, renderType="normal") {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const [index, [key, value]] of Object.entries(data.movies ?? {}).entries()) {
    const button = document.createElement("button");

    if (renderType==="normal") {
        button.textContent = `${value}`;
        button.dataset.id = key;
        button.id = `${element_id}_${key}`;
    } else if (renderType==="enumerated") {
        button.textContent = `${key} (${value.toFixed(2)})`;
        button.dataset.id = index;
        button.id = `${element_id}_${index}`;
    }
    button.className = "option";

    container.appendChild(button);
  }
}

function paginationClick(type, page) {
    const userId = getFromUrl("user", null)
    function_to_call[type](userId, page)
    setInUrl(`${type}_page`, page)
}

export function set_pagination(type, page, totalPages, hasNext, hasPrev) {
    const elementId = `${type}_pagination`
    const pagination = document.getElementById(elementId);
    pagination.innerHTML = "";

    // Add numbers buttons
    const start = Math.max(page - 2, 1)
    const end = Math.min(start + 4, totalPages)

    // Add prev button
    const prevButton = document.createElement("button");
    prevButton.textContent = "Previous";
    prevButton.dataset.page = start - 1;

    prevButton.classList.add("pagination-button");
    prevButton.classList.toggle("disabled", !hasPrev);
    prevButton.disabled = !hasPrev;

    prevButton.id = `${elementId}_prev`;
    prevButton.addEventListener("click", () => {
        paginationClick(type, page - 1)
    });

    pagination.appendChild(prevButton);

    for (let i = start; i <= end; i++) {
        const numberButton = document.createElement("button");
        numberButton.textContent = i;
        numberButton.dataset.page = i;

        numberButton.classList.add("pagination-button");
        numberButton.classList.toggle("active", i === page);

        numberButton.id = `${elementId}_${i}`;
        numberButton.addEventListener("click", () => {
            paginationClick(type, i)
        });

        pagination.appendChild(numberButton);
    }

    // Add next button
    const nextButton = document.createElement("button");
    nextButton.textContent = "Next";
    nextButton.dataset.page = page + 4;

    nextButton.classList.add("pagination-button");
    nextButton.classList.toggle("disabled", !hasNext);
    nextButton.disabled = !hasNext;

    nextButton.id = `${elementId}_next`;
    nextButton.addEventListener("click", () => {
        paginationClick(type, page + 1)
    });

    pagination.appendChild(nextButton);
}

function renderSelection(id, elementId, clear=true) {
    const container = document.getElementById(elementId);

    if (clear) {
        container
            .querySelectorAll("button.selected")
            .forEach((btn) => btn.classList.remove("selected"));
    }

    const el = container.querySelector(`#${elementId}_${id}`);
    if (el) {
        el.classList.add("selected");
    }
}

async function loadRecommendations(userId, page=1, pageSize=20) {
    
    const type = "recommendations";
    const data = await call_api(type, userId, page, pageSize);

    renderDict(data, type, "enumerated");

    set_pagination(type, page, data.total_pages, data.has_next, data.has_prev);

    setInUrl("show_recs", true)
}


const button = document.getElementById("get_recommendations");

button.addEventListener("click", () => {
    const userId = getFromUrl("user", null)
    if (!userId) {
        alert("Please select a user first");
        return;
    }
    loadRecommendations(userId)
});
