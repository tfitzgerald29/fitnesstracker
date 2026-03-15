// Load FullCalendar from CDN, init when ready
(function () {
  var link = document.createElement("link");
  link.rel = "stylesheet";
  link.href =
    "https://cdn.jsdelivr.net/npm/fullcalendar@6.1.11/index.global.min.css";
  document.head.appendChild(link);

  var script = document.createElement("script");
  script.src =
    "https://cdn.jsdelivr.net/npm/fullcalendar@6.1.11/index.global.min.js";
  script.onload = function () {
    window._fcLoaded = true;
    tryInit();
  };
  document.head.appendChild(script);

  window._fcCalendar = null;
  window._fcCurrentMonth = null;

  var SPORT_LABELS = {
    cycling: "Cycling",
    weight_lifting: "Lifting",
    rock_climbing: "Rock Climbing",
    running: "Running",
    hiking: "Hiking",
    alpine_skiing: "Skiing",
    generic: "Other",
    racket: "Racket",
  };

  var SPORT_COLORS = {
    cycling: "#2196F3",
    weight_lifting: "#FF9800",
    rock_climbing: "#4CAF50",
    running: "#E91E63",
    hiking: "#8BC34A",
    alpine_skiing: "#00BCD4",
    generic: "#9E9E9E",
    racket: "#AB47BC",
  };

  function summarize(raw, startDate, endDate) {
    var totals = {};
    for (var i = 0; i < raw.length; i++) {
      var r = raw[i];
      if (r.date >= startDate && r.date <= endDate) {
        if (!totals[r.sport]) {
          totals[r.sport] = { count: 0, seconds: 0, miles: 0 };
        }
        totals[r.sport].count += 1;
        totals[r.sport].seconds += r.seconds;
        totals[r.sport].miles += r.miles;
      }
    }
    return totals;
  }

  function pad2(n) {
    return n < 10 ? "0" + n : "" + n;
  }

  function toISO(d) {
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  var MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
  ];

  // --- Summary bar builder (reused for week + month rows) ---

  function buildBarHTML(heading, totals) {
    var keys = Object.keys(totals);
    if (keys.length === 0) {
      return '<span style="color:#555;font-weight:600">' + heading + ": 0 activities</span>";
    }

    var entries = keys.map(function (sport) {
      return { sport: sport, stats: totals[sport] };
    });
    entries.sort(function (a, b) {
      return b.stats.count - a.stats.count;
    });

    var totalCount = 0;
    var totalSec = 0;
    for (var i = 0; i < keys.length; i++) {
      totalCount += totals[keys[i]].count;
      totalSec += totals[keys[i]].seconds;
    }
    var totalHours = (totalSec / 3600).toFixed(1);

    var chips = "";
    for (var i = 0; i < entries.length; i++) {
      var sport = entries[i].sport;
      var stats = entries[i].stats;
      var color = SPORT_COLORS[sport] || "#9E9E9E";
      var label =
        SPORT_LABELS[sport] || sport.replace(/_/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
      var parts = [stats.count + "x"];
      var h = stats.seconds / 3600;
      if (h > 0) parts.push(h.toFixed(1) + "h");
      if (stats.miles > 1) parts.push(Math.round(stats.miles) + "mi");

      chips +=
        '<span style="display:inline-flex;align-items:center;gap:4px;margin-right:12px">' +
        '<span style="width:7px;height:7px;border-radius:2px;background:' + color + ';display:inline-block"></span>' +
        '<span style="color:#bbb">' + label + "</span>" +
        '<span style="color:#777">' + parts.join(" · ") + "</span>" +
        "</span>";
    }

    return (
      '<span style="color:#e0e0e0;font-weight:600;margin-right:16px">' + heading + ": " +
      totalCount + " activities · " + totalHours + "h</span>" +
      chips
    );
  }

  // --- Inject week + month summary rows ---

  function injectSummaryRows(raw) {
    // Remove any previously injected rows
    var old = document.querySelectorAll(".fc-week-summary-row, .fc-month-summary-row");
    for (var i = 0; i < old.length; i++) {
      old[i].parentNode.removeChild(old[i]);
    }

    var tbody = document.querySelector(".fc .fc-daygrid-body tbody");
    if (!tbody) return;

    var weekTrs = tbody.querySelectorAll("tr");
    var lastWeekTr = null;

    for (var w = 0; w < weekTrs.length; w++) {
      var tr = weekTrs[w];
      var dayCells = tr.querySelectorAll("td.fc-daygrid-day[data-date]");
      if (dayCells.length === 0) continue;

      var dates = [];
      for (var d = 0; d < dayCells.length; d++) {
        var dt = dayCells[d].getAttribute("data-date");
        if (dt) dates.push(dt);
      }
      if (dates.length === 0) continue;

      dates.sort();
      lastWeekTr = tr;

      // Use the actual date range visible in this calendar row
      var startDate = dates[0];
      var endDate = dates[dates.length - 1];

      var weekTotals = summarize(raw, startDate, endDate);
      var barHTML = buildBarHTML("Week", weekTotals);

      var summaryTr = document.createElement("tr");
      summaryTr.className = "fc-week-summary-row";
      var td = document.createElement("td");
      td.colSpan = 7;
      td.innerHTML = barHTML;
      summaryTr.appendChild(td);

      if (tr.nextSibling) {
        tbody.insertBefore(summaryTr, tr.nextSibling);
      } else {
        tbody.appendChild(summaryTr);
      }
    }

    // Add month total after the very last week row of the month
    if (lastWeekTr && window._fcCurrentMonth) {
      var parts = window._fcCurrentMonth.split("-");
      var year = parseInt(parts[0]);
      var month = parseInt(parts[1]) - 1;
      var monthStart = new Date(year, month, 1);
      var monthEnd = new Date(year, month + 1, 0);
      var monthTotals = summarize(raw, toISO(monthStart), toISO(monthEnd));
      var monthLabel = MONTH_NAMES[month];
      var monthBarHTML = buildBarHTML(monthLabel + " Total", monthTotals);

      if (monthBarHTML) {
        var monthTr = document.createElement("tr");
        monthTr.className = "fc-month-summary-row";
        var monthTd = document.createElement("td");
        monthTd.colSpan = 7;
        monthTd.innerHTML = monthBarHTML;
        monthTr.appendChild(monthTd);

        // Append at the very end of the tbody
        tbody.appendChild(monthTr);
      }
    }
  }

  // --- Calendar init ---

  function tryInit() {
    if (!window._fcLoaded) return;
    var el = document.getElementById("fc-container");
    if (!el) return;

    var eventsEl = document.getElementById("fc-events-data");
    if (!eventsEl) return;

    var rawEl = document.getElementById("fc-raw-data");
    if (!rawEl) return;

    var events = [];
    var raw = [];
    var initialDate = null;
    try {
      events = JSON.parse(eventsEl.textContent);
      raw = JSON.parse(rawEl.textContent);
      var initEl = document.getElementById("fc-initial-date");
      if (initEl) initialDate = JSON.parse(initEl.textContent);
    } catch (e) {
      return;
    }

    if (window._fcCalendar) {
      window._fcCalendar.destroy();
      window._fcCalendar = null;
    }

    var calOpts = {
      initialView: "dayGridMonth",
      firstDay: 1,
      fixedWeekCount: false,
      showNonCurrentDates: true,
      headerToolbar: {
        left: "prev,next today",
        center: "title",
        right: "",
      },
      events: events,
      editable: false,
      selectable: false,
      nowIndicator: true,
      height: "auto",
      themeSystem: "standard",
      datesSet: function (info) {
        window._fcCurrentMonth = info.view.currentStart.toISOString().slice(0, 10);
        setTimeout(function () { injectSummaryRows(raw); }, 0);
      },
    };
    if (initialDate) calOpts.initialDate = initialDate;

    window._fcCalendar = new FullCalendar.Calendar(el, calOpts);
    window._fcCalendar.render();
  }

  var observer = new MutationObserver(function () {
    var el = document.getElementById("fc-container");
    if (el && !window._fcCalendar) {
      tryInit();
    }
    if (
      el &&
      window._fcCalendar &&
      !document.contains(window._fcCalendar.el)
    ) {
      window._fcCalendar = null;
      tryInit();
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
})();
