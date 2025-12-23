export function renderSelection(selection, elementId, clear = true) {
    const container = document.getElementById(elementId);
    if (!container) return; // safety check

    // Clear previously selected buttons if needed
    if (clear) {
        container
            .querySelectorAll("button.selected")
            .forEach((btn) => btn.classList.remove("selected"));
    }

    // Return early if selection is null/undefined/empty string
    if (selection == null || selection === "") {
        return;
    }

    let selections;

    if (typeof selection === "string" && selection.includes(",")) {
        // Split string by commas and trim whitespace
        selections = selection.split(",").map(s => s.trim());
    } else if (Array.isArray(selection)) {
        // Keep single value as array
        selections = selection;
    } else {
        // Keep single value as array
        selections = [selection];
    }

    // Add "selected" class to each element
    selections.forEach((singleSelection) => {
        const el = container.querySelector(`#${elementId}_${singleSelection}`);
        if (el) {
            el.classList.add("selected");
        }
    });
}
