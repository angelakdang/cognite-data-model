import random
from dataclasses import dataclass
from typing import List, Tuple

import arrow
import yaml
from cognite.client.data_classes import TimeSeries

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


def generate_random_data(
    start: arrow.Arrow = None,
    end: arrow.Arrow = None,
    granularity: int = 1,
    value_range: Tuple[int, int] = (0, 1),
) -> List[Tuple[float, float]]:
    if start is None:
        start = arrow.utcnow().shift(hours=-24)
    if end is None:
        end = arrow.utcnow()

    output = []

    current_time = start.floor("minute")
    while current_time < end:
        timestamp = current_time.timestamp() * 1000
        value = random.uniform(*value_range)
        output.append((int(timestamp), value))
        current_time = current_time.shift(minutes=granularity)

    return output


# Load the YAML file into a list of data class instances
heat_exchangers = []
with open("data.yaml", "r") as file:
    data = yaml.safe_load(file)
    for d in data:
        heat_exchangers.append(HeatExchanger(**d))

# Generate timeseries data in CDF
ts_xids = []
for hx in heat_exchangers:
    ts_xids.extend(
        # List of tuples containing values that will be used for TimeSeries name, metadata "type", and external_id
        [
            (f"{hx.name}_coolWatSupplyTemp", "coolWatSupplyTemp", hx.coolWatSupplyTemp),
            (f"{hx.name}_coolWatReturnTemp", "coolWatReturnTemp", hx.coolWatReturnTemp),
            (f"{hx.name}_processSupplyTemp", "processSupplyTemp", hx.processSupplyTemp),
            (f"{hx.name}_processReturnTemp", "processReturnTemp", hx.processReturnTemp),
            (f"{hx.name}_coolWatFlow", "coolWatFlow", hx.coolWatFlow),
        ]
    )
    if hx.enabler:
        ts_xids.append((f"{hx.name}_enabler", "enabler", hx.enabler))

# For each timeseries, create a CDF timeseries
timeseries = [
    TimeSeries(external_id=xid, name=name, metadata={"type": ts_type})
    for (name, ts_type, xid) in ts_xids
]
existing_timeseries = [ts.external_id for ts in client.time_series.list(limit=None)]
timeseries_to_create = [
    ts for ts in timeseries if ts.external_id not in existing_timeseries
]
client.time_series.create(timeseries_to_create)

# Populate the timeseries
now = arrow.utcnow()
datapoints = []
# Get latest datapoints for each timeseries
latest_datapoints = {
    dp.external_id: dp.timestamp
    for dp in client.time_series.data.retrieve_latest(
        external_id=[ts.external_id for ts in timeseries], ignore_unknown_ids=False
    )
}

for ts in timeseries:
    if latest_datapoints.get(ts.external_id, None):
        try:
            # Get latest data point and set it as the start date
            latest_datapoint = latest_datapoints[ts.external_id][0]
            datapoints.append(
                {
                    "external_id": ts.external_id,
                    "datapoints": generate_random_data(
                        start=arrow.get(latest_datapoint), end=now
                    ),
                }
            )
        except IndexError:
            # No latest datapoint retrieved
            datapoints.append(
                {
                    "external_id": ts.external_id,
                    "datapoints": generate_random_data(end=now),
                }
            )
    else:
        datapoints.append(
            {"external_id": ts.external_id, "datapoints": generate_random_data(end=now)}
        )
client.time_series.data.insert_multiple(datapoints)
