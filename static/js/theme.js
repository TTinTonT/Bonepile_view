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
    // Support both legacy button/link toggle and the new checkbox switch.
    var isDark = theme === "dark";
    var tag = (btn.tagName || "").toLowerCase();
    var type = (btn.getAttribute && btn.getAttribute("type")) || "";

    if (tag === "input" && type.toLowerCase() === "checkbox") {
      btn.checked = isDark; // checked = Dark
      return;
    }

    // Legacy fallback: label shows the ACTION (what you'll switch to)
    var isLight = theme === "light";
    btn.textContent = isLight ? "Dark" : "Light";
    btn.setAttribute("aria-pressed", isDark ? "true" : "false");
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
    if (!btn) return;

    var tag = (btn.tagName || "").toLowerCase();
    var type = (btn.getAttribute && btn.getAttribute("type")) || "";
    if (tag === "input" && type.toLowerCase() === "checkbox") {
      btn.addEventListener("change", function () {
        setTheme(btn.checked ? "dark" : "light");
      });
      return;
    }

    btn.addEventListener("click", function (e) {
      if (e && typeof e.preventDefault === "function") e.preventDefault();
      toggleTheme();
    });
  });
})();

