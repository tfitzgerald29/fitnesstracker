import os

from dotenv import load_dotenv

load_dotenv()

# ── Supabase config ────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")


# ── Supabase client ────────────────────────────────────────────────────
def get_supabase_client():
    """Return a Supabase client instance.

    Instantiated on demand so the app still starts locally even without
    SUPABASE_URL / SUPABASE_ANON_KEY set.
    """
    from supabase import create_client

    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
