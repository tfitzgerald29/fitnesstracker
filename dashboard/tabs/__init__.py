from .calendar import calendar_tab  # noqa: F401
from .climbing import climbing_tab, update_climbing_overview, update_climbing_session  # noqa: F401
from .cycling import cycling_tab  # noqa: F401 — importing the module registers all @callback decorators
from .sports import sports_tab, update_sport_summary, update_summary_cards, update_total_chart, update_sport_chart  # noqa: F401
from .weights import weights_tab, render_weights_subtab, update_exercise_progress, handle_sets, handle_workout  # noqa: F401
