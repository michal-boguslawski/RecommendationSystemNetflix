export function setInUrl(key, value) {
    const url = new URL(window.location);
    url.searchParams.set(key, value);
    history.pushState({}, "", url)
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

export function clearContainer(element_id) {
    const container = document.getElementById(element_id);
    container.innerHTML = "";
}
