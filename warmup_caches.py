import os

from backend.cycling_processor import CyclingProcessor


def main() -> None:
    user_id = os.environ.get("FITNESS_USER_ID") or None

    print("Starting cache warmup...")
    cp = CyclingProcessor(user_id=user_id)
    cp.warm_startup_caches(n_bootstrap=5000)
    print("Cache warmup complete.")


if __name__ == "__main__":
    main()
