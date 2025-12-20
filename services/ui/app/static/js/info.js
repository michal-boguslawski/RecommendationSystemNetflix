async function loadUsers() {
  const cached = sessionStorage.getItem(`users`);
  if (cached) {
    renderList(JSON.parse(cached), "users");
    return;
  }

  try {
    const response = await fetch(
      `http://localhost:8000/list_users/`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    // Cache in sessionStorage
    sessionStorage.setItem(`users`, JSON.stringify(data));

    renderList(data, "users");
  } catch (err) {
    console.error("Failed to load users:", err);
    alert("Could not load users");
  }
}

async function loadMovies() {
  const cached = sessionStorage.getItem(`movies`);
  if (cached) {
    renderDict(JSON.parse(cached), "movies");
    return;
  }

  try {
    const response = await fetch(
      `http://localhost:8000/list_movies/`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    // Cache in sessionStorage
    sessionStorage.setItem(`movies`, JSON.stringify(data));

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
    const li = document.createElement("li");
    li.textContent = user_id;
    container.appendChild(li);
  }
}

function renderDict(data, element_id) {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const [movieId, movieName] of Object.entries(data.movies ?? {})) {
    const li = document.createElement("li");
    li.textContent = `${movieName} (${movieId})`;
    container.appendChild(li);
  }
}