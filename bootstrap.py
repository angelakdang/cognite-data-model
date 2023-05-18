import random
from dataclasses import dataclass

import arrow
import yaml

from auth import get_cognite_client

client = get_cognite_client()


# Define the data class for HeatExchangers
@dataclass
class HeatExchanger:
    name: str
    coolWatSupplyTemp: str
    coolWatReturnTemp: str
    processSupplyTemp: str
    processReturnTemp: str
    coolWatFlow: str
    designArea: int
    subFlows: int
    enabler: str = None
    enablerLimit: int = 0


def generate_random_data(time: arrow.Arrow):
    current_time = time
    output = []

    for _ in range(60):
        timestamp = current_time.timestamp() * 1000
        value = random.random()
        data.append((int(timestamp), value))
        current_time = current_time.shift(minutes=-1)

    return output


# Load the YAML file into a list of data class instances
heat_exchangers = []
with open("data.yaml", "r") as file:
    data = yaml.safe_load(file)
    for d in data:
        heat_exchangers.append(HeatExchanger(**d))

# Generate timeseries data in CDF
now = arrow.utcnow()
timeseries = []
for hx in heat_exchangers:
    timeseries.extend(
        [
            hx.coolWatSupplyTemp,
            hx.coolWatReturnTemp,
            hx.processSupplyTemp,
            hx.processReturnTemp,
            hx.coolWatFlow,
        ]
    )
    if hx.enabler:
        timeseries.append(hx.enabler)

print(timeseries)
for ts in timeseries:
    pass
