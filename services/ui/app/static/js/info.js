document.addEventListener("DOMContentLoaded", () => {
  loadUsers().then(() => {
    const userId = sessionStorage.getItem("current_userId");
    if (!userId) return;

    const button = document.querySelector(
      `#users button[data-user_id="${userId}"]`
    );

    if (button) {
      button.classList.add("selected");
      updateMoviesTitle(userId);
      loadRatedMovies(userId);
    }
  })
});

async function loadUsers(page = 1, pageSize = 20) {
  try {
    const response = await fetch(
      `http://localhost:8000/list_users/?page=${page}&pageSize=${pageSize}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    renderList(data, "users");

    // Return data so you can chain actions
    return data;
  } catch (err) {
    console.error("Failed to load users:", err);
    alert("Could not load users");
  }
}

async function loadMovies(page = 1, pageSize = 20) {
  try {
    const response = await fetch(
      `http://localhost:8000/list_movies/?page=${page}&pageSize=${pageSize}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    renderDict(data, "movies");
  } catch (err) {
    console.error("Failed to load movies:", err);
    alert("Could not load movies");
  }
}

function renderList(data, element_id) {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const user_id of data.users ?? []) {
    const button = document.createElement("button");

    button.textContent = user_id;
    button.dataset.user_id = user_id;
    button.className = "option";
    button.id = `${element_id}_${user_id}`;

    container.appendChild(button);
  }
}

function renderDict(data, element_id) {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const [movieId, movieName] of Object.entries(data.movies ?? {})) {
    const button = document.createElement("button");

    button.textContent = `${movieName}`;
    button.dataset.user_id = movieId;
    button.className = "option";
    button.id = `${element_id}_${movieId}`;

    container.appendChild(button);
  }
}