const root = document.documentElement;

function setTheme(theme) {
    root.setAttribute("data-theme", theme);
    localStorage.setItem("theme", theme);
}

function toggleTheme() {
    const current = localStorage.getItem("theme") || "light";
    setTheme(current === "light" ? "dark" : "light");
}

(function initTheme() {
    const saved = localStorage.getItem("theme") || "light";
    setTheme(saved);
})();