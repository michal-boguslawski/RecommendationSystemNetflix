export function setInUrl(type, movies) {
    const url = new URL(window.location);
    
    // Remove existing movies params
    url.searchParams.delete(type);

    if (Array.isArray(movies)) {
        movies.forEach(m => url.searchParams.append(type, m));
    } else {
        url.searchParams.append(type, movies);
    }

    // Update the URL in the browser without reloading
    window.history.replaceState({}, "", url);
}

export function getFromUrl(key, dflt) {
    const url = new URL(window.location);
    return url.searchParams.get(key) || dflt;
}

export function removeFromUrl(key) {
    const url = new URL(window.location);
    url.searchParams.delete(key);
    history.pushState({}, "", url)
}

export function getAllFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const result = {};

    for (const [key, value] of params.entries()) {
        if (result[key]) {
            // If already exists, convert to array (or push)
            if (Array.isArray(result[key])) {
                result[key].push(value);
            } else {
                result[key] = [result[key], value];
            }
        } else {
            result[key] = value;
        }
    }

    return result;
}
