const uri = "http://localhost:8000"

const endpoint_dict = {
    "users": "users",
    "movies": "movies",
    "rated_movies": "movies",
    "recommendations": "recommend",
}

export async function call_api({ type, id, page=1, page_size=20, movies }) {
    let url = `${uri}/${endpoint_dict[type]}`

    if (id) {
        url += `/${id}`
    }
    url += `?page=${page}&pageSize=${page_size}`

    if (movies) {
        if (Array.isArray(movies)) {
            // Add each movie as its own query param
            movies.forEach((m) => {
                url += `&movies=${encodeURIComponent(m)}`;
            });
        } else {
            // Single movie
            url += `&movies=${encodeURIComponent(movies)}`;
        }
    }

    console.log("API call:", url)

    try {
        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();
        console.log("API response for", type, ":", data)

        return data;
    } catch (err) {
        console.error("Failed to load movies:", err);
        alert("Could not load movies");
    }
}
