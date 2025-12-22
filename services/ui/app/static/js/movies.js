import { set_pagination } from "./pagination.js";

document.addEventListener("DOMContentLoaded", () => {
  loadRatedMovies();
});

document.getElementById("get_recommendations").addEventListener("click", () => {
  const userId = sessionStorage.getItem("current_userId");

  if (!userId) {
    alert("Please select a user first");
    return;
  }

  loadRecommendations(userId);
});

const usersContainer = document.getElementById("users");

usersContainer.addEventListener("click", (e) => {
  console.log("Click event:", e.target);
  const button = e.target.closest("button[data-user_id]");
  console.log("Found button:", button);
  if (!button) return;

  const userId = button.dataset.user_id;
  console.log("Clicked userId:", userId);

  usersContainer
    .querySelectorAll("button.selected")
    .forEach((btn) => btn.classList.remove("selected"));

  button.classList.add("selected");
  sessionStorage.setItem("current_userId", userId);

  loadRatedMovies(userId);
});

async function loadRatedMovies(userId) {
  if (!userId) {
    userId = sessionStorage.getItem("current_userId");
  }

  if (!userId) {
    console.warn("No userId provided or stored");
    updateMoviesTitle(null);
    return;
  }

  sessionStorage.setItem("current_userId", userId);
  updateMoviesTitle(userId);

  try {
    const response = await fetch(
      `http://localhost:8000/list_movies/${userId}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    renderRatings(data, "rated_movies");
    set_pagination("rated_movies_pagination", data.page, data.total_pages, data.has_next, data.has_prev);
  } catch (err) {
    console.error("Failed to load movies:", err);
    alert("Could not load movies");
  }
}

async function loadRecommendations(userId) {

  try {
    const response = await fetch(
      `http://localhost:8000/recommend/${userId}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    renderRatings(data, "recommendations");
    set_pagination("recommendations_pagination", data.page, data.total_pages, data.has_next, data.has_prev);
  } catch (err) {
    console.error("Failed to load movies:", err);
    alert("Could not load movies");
  }
}

function renderRatings(data, element_id) {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const [movieName, rating] of Object.entries(data.movies ?? {})) {
    const button = document.createElement("button");

    button.textContent = `${movieName} (${rating.toFixed(2)})`;
    button.value = rating;
    button.className = "option";
    button.id = `${element_id}_${movieName}`;

    container.appendChild(button);
  }
}

function updateMoviesTitle(userId) {
  const title = document.getElementById("moviesTitle");

  if (!userId) {
    title.textContent = "Recommended movies";
    return;
  }

  title.textContent = `Recommended movies for user ${userId}`;
}
