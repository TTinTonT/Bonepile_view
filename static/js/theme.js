(function () {
  function getStoredTheme() {
    try {
      return localStorage.getItem("theme");
    } catch (e) {
      return null;
    }
  }

  function getPreferredTheme() {
    var stored = getStoredTheme();
    if (stored === "light" || stored === "dark") return stored;

    try {
      if (window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches) {
        return "light";
      }
    } catch (e) {
      // ignore
    }
    return "dark";
  }

  function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem("theme", theme);
    } catch (e) {
      // ignore
    }
    updateToggle(theme);
  }

  function updateToggle(theme) {
    var btn = document.getElementById("themeToggle");
    if (!btn) return;
    var isLight = theme === "light";
    btn.textContent = isLight ? "Light" : "Dark";
    btn.setAttribute("aria-pressed", isLight ? "true" : "false");
    btn.title = isLight ? "Switch to Dark" : "Switch to Light";
  }

  function toggleTheme() {
    var current = document.documentElement.getAttribute("data-theme");
    setTheme(current === "light" ? "dark" : "light");
  }

  document.addEventListener("DOMContentLoaded", function () {
    var theme = document.documentElement.getAttribute("data-theme");
    if (theme !== "light" && theme !== "dark") theme = getPreferredTheme();
    setTheme(theme);

    var btn = document.getElementById("themeToggle");
    if (btn) btn.addEventListener("click", toggleTheme);
  });
})();

