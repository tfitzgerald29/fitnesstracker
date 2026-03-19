// Keep Dash tab bars usable across phone orientation changes.
(function () {
  var TAB_IDS = ["tabs", "cycling-subtabs", "weights-subtabs"];
  var portraitMq = window.matchMedia("(orientation: portrait)");
  var narrowMq = window.matchMedia("(max-width: 900px)");

  function getTabList(root) {
    return root.querySelector('[role="tablist"]') || root.querySelector(".tab-list");
  }

  function normalizeTabStrip(listEl) {
    listEl.style.display = "flex";
    listEl.style.alignItems = "center";
    listEl.style.justifyContent = "flex-start";
    listEl.style.overflowX = "auto";
    listEl.style.overflowY = "hidden";
    listEl.style.whiteSpace = "nowrap";
    listEl.style.WebkitOverflowScrolling = "touch";
    listEl.style.scrollbarWidth = "thin";
    listEl.style.width = "100%";
  }

  function normalizeTabs(root) {
    var tabs = root.querySelectorAll('[role="tab"], .tab');
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].style.flex = "0 0 auto";
      tabs[i].style.whiteSpace = "nowrap";
      if (narrowMq.matches) {
        tabs[i].style.fontSize = "0.85rem";
        tabs[i].style.padding = "6px 10px";
      } else {
        tabs[i].style.fontSize = "";
        tabs[i].style.padding = "";
      }
    }
  }

  function keepSelectedVisible(root, reason) {
    var listEl = getTabList(root);
    if (!listEl) return;

    if (root.id === "tabs" && (reason === "init" || reason === "portrait")) {
      listEl.scrollLeft = 0;
      return;
    }

    var selected = root.querySelector('[role="tab"][aria-selected="true"], .tab--selected');
    if (!selected) return;

    try {
      selected.scrollIntoView({ inline: "nearest", block: "nearest" });
    } catch (e) {
      selected.scrollIntoView();
    }
  }

  function syncTabBars(reason) {
    for (var i = 0; i < TAB_IDS.length; i++) {
      var root = document.getElementById(TAB_IDS[i]);
      if (!root) continue;
      var listEl = getTabList(root);
      if (!listEl) continue;
      normalizeTabStrip(listEl);
      normalizeTabs(root);
      keepSelectedVisible(root, reason);
    }
  }

  var resizeTimer = null;
  function onResize() {
    if (resizeTimer) clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      syncTabBars("resize");
    }, 120);
  }

  function onOrientationChange() {
    syncTabBars(portraitMq.matches ? "portrait" : "landscape");
  }

  if (typeof portraitMq.addEventListener === "function") {
    portraitMq.addEventListener("change", onOrientationChange);
  } else if (typeof portraitMq.addListener === "function") {
    portraitMq.addListener(onOrientationChange);
  }

  if (typeof narrowMq.addEventListener === "function") {
    narrowMq.addEventListener("change", onResize);
  } else if (typeof narrowMq.addListener === "function") {
    narrowMq.addListener(onResize);
  }

  window.addEventListener("resize", onResize);

  var observer = new MutationObserver(function () {
    syncTabBars("mutation");
  });
  observer.observe(document.body, { childList: true, subtree: true });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      syncTabBars("init");
    });
  } else {
    syncTabBars("init");
  }
})();
