import json
from datetime import datetime
from utils.monitoring import monitor

@monitor
def update_lineage(input_path, output_path):
    pass
    # lineage = {
    #     "timestamp": str(datetime.now()),
    #     "source": input_path,
    #     "target": output_path,
    #     "transformations": [
    #         "drop nulls",
    #         "rename columns",
    #         "feature engineering (hour, day_of_week)"
    #     ]
    # }

    # with open("docs/lineage.json", "w") as f:
    #     json.dump(lineage, f, indent=2)

    # print("Lineage logged")