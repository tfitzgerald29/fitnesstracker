import json
import os

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DATA_FILE = os.path.join(
    _BASE_DIR, "weighttraining_data", "weighttraining_data.json"
)


class WeightTrainingLog:
    def __init__(self, data_file=None):
        self.data_file = data_file or _DEFAULT_DATA_FILE
        self.log = self._load()

    def _load(self):
        if os.path.exists(self.data_file):
            with open(self.data_file, "r") as f:
                return json.load(f)
        return []

    def save(self):
        with open(self.data_file, "w") as f:
            json.dump(self.log, f, indent=2)
        print(f"Saved {len(self.log)} total entries to {self.data_file}")

    def get_exercise_names(self):
        return sorted({ex["name"] for entry in self.log for ex in entry["exercises"]})

    def print_exercise_names(self):
        names = self.get_exercise_names()
        print(f"\nExisting exercise names ({len(names)}):")
        for name in names:
            print(f"  - {name}")
        print()

    def add_entry(self, date, exercises):
        existing = next((entry for entry in self.log if entry["date"] == date), None)
        if existing:
            existing["exercises"].extend(exercises)
            print(f"Appended to existing entry for {date}")
        else:
            self.log.append({"date": date, "exercises": exercises})
            print(f"Created new entry for {date}")

        for ex in exercises:
            print(f"  {ex['name']}:")
            for s in ex["sets"]:
                print(f"    Set {s['set']}: {s['reps']} reps @ {s['weight']} lbs")

        self.save()
        print(f"\nAdded {len(exercises)} exercises for {date}")


if __name__ == "__main__":
    wt = WeightTrainingLog()
    print(f"Data file: {wt.data_file}")
    print(f"Existing entries: {len(wt.log)}")
    wt.print_exercise_names()

    # --- Add a workout day ---
    date = "2026-02-21"
    exercises = [
        {
            "name": "shrugs",
            "sets": [
                {"set": 1, "weight": 65, "reps": 15},
                {"set": 2, "weight": 65, "reps": 15},
                {"set": 3, "weight": 65, "reps": 15},
            ],
        },
        {
            "name": "ab_wheel",
            "sets": [
                {"set": 1, "weight": 0, "reps": 20},
                {"set": 2, "weight": 0, "reps": 20},
                {"set": 3, "weight": 0, "reps": 20},
            ],
        },
        {
            "name": "dragon_flags",
            "sets": [
                {"set": 1, "weight": 0, "reps": 10},
                {"set": 2, "weight": 0, "reps": 10},
                {"set": 3, "weight": 0, "reps": 10},
            ],
        },
        {
            "name": "ezbarcurl",
            "sets": [
                {"set": 1, "weight": 35, "reps": 10},
                {"set": 2, "weight": 35, "reps": 10},
                {"set": 3, "weight": 35, "reps": 10},
            ],
        },
        {
            "name": "hammer_curl",
            "sets": [
                {"set": 1, "weight": 25, "reps": 8},
                {"set": 2, "weight": 30, "reps": 8},
                {"set": 3, "weight": 30, "reps": 8},
            ],
        },
    ]

    wt.add_entry(date, exercises)
