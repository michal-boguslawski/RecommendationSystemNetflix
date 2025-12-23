import { clickUsers, clickMovie, clickPagination } from "./handlers.js";
import { renderSelection } from "./render_shared.js"


export function renderList(data, element_id, selection=null) {
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
    renderSelection(selection, element_id, false);
}


export function renderDict(data, element_id, selection=null) {
    const container = document.getElementById(element_id);
    container.innerHTML = "";

    // adjust based on API response shape

    for (const [key, value] of Object.entries(data.movies ?? {})) {
        const button = document.createElement("button");
        let textContent;

        if (typeof value === "string") {
            textContent = value;
        } else {
            textContent = `${value["MovieName"]} (${Number(value["Rating"]).toFixed(2)})`;
        }
        button.textContent = textContent;
        button.dataset.id = key;
        button.id = `${element_id}_${key}`;

        if (element_id==="movies") {
            button.addEventListener("click", () => {
                console.log("Clicked ", element_id, ": ", key);
                clickMovie(element_id, key);
            })
        }

        button.className = "option";

        container.appendChild(button);
    }

    renderSelection(selection, element_id, false);
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
        clickPagination(type, page - 1)
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
            clickPagination(type, i)
        });

        pagination.appendChild(numberButton);
    }

    // Add next button
    const nextButton = document.createElement("button");
    nextButton.textContent = "Next";
    nextButton.dataset.page = Number(page) + 4;

    nextButton.classList.add("pagination-button");
    nextButton.classList.toggle("disabled", !hasNext);
    nextButton.disabled = !hasNext;

    nextButton.id = `${elementId}_next`;
    nextButton.addEventListener("click", () => {
        clickPagination(type, page + 1)
    });

    pagination.appendChild(nextButton);
}
