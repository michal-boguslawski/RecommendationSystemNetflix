async function loadRatedMovies(userId) {
  const cached = sessionStorage.getItem(`rated_movies_${userId}`);
  if (cached) {
    renderMovies(JSON.parse(cached), "rated_movies");
    return;
  }

  try {
    const response = await fetch(
      `http://localhost:8000/list_movies/${userId}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    // Cache in sessionStorage
    sessionStorage.setItem(`rated_movies_${userId}`, JSON.stringify(data));

    renderMovies(data, "rated_movies");
  } catch (err) {
    console.error("Failed to load movies:", err);
    alert("Could not load movies");
  }
}

async function loadRecommendations(userId) {
  const cached = sessionStorage.getItem(`recommendation_movies_${userId}`);
  if (cached) {
    renderMovies(JSON.parse(cached), "recommendations");
    return;
  }
  try {
    const response = await fetch(
      `http://localhost:8000/recommend/${userId}`
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    console.log("API response:", data);

    // Cache in sessionStorage
    sessionStorage.setItem(`recommendation_movies_${userId}`, JSON.stringify(data));

    renderMovies(data, "recommendations");
  } catch (err) {
    console.error("Failed to load movies:", err);
    alert("Could not load movies");
  }
}

function renderMovies(data, element_id) {
  const container = document.getElementById(element_id);
  container.innerHTML = "";

  // adjust based on API response shape

  for (const [movieName, rating] of Object.entries(data.movies ?? {})) {
    const li = document.createElement("li");
    li.textContent = `${movieName} (${rating.toFixed(2)})`;
    container.appendChild(li);
  }
}
