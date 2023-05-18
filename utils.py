import random
from dataclasses import dataclass
from typing import List, Tuple

import arrow
import numpy as np
import yaml


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


def get_heat_exchanger_data() -> List[HeatExchanger]:
    # Load the YAML file into a list of data class instances
    heat_exchangers = []
    with open("data.yaml", "r") as file:
        data = yaml.safe_load(file)
        for d in data:
            heat_exchangers.append(HeatExchanger(**d))
    return heat_exchangers


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


def calculate_u_value(
    cool_wat_supply_temp: float,
    cool_wat_return_temp: float,
    process_supply_temp: float,
    process_return_temp: float,
    cool_wat_flow: float,
    area: float,
    enabler: float = 1,
    var1: float = 500.0,  # TODO: Define what this is
    max_value: float = 300.0,
    min_value: float = 0.0,
):
    # TODO: Remove these conditional statements, just a dummy thing to make the calculations look better
    if cool_wat_supply_temp > process_supply_temp:
        process_supply_temp += cool_wat_supply_temp
    if cool_wat_return_temp > process_return_temp:
        process_return_temp += cool_wat_return_temp

    # Begin actual calculations
    q = (
        cool_wat_flow * var1 * (cool_wat_return_temp - cool_wat_supply_temp)
    )  # total heat transfer
    ratio = (process_supply_temp - cool_wat_supply_temp) / (
        process_return_temp - cool_wat_return_temp
    )
    dt_log_mean = ratio * np.log(ratio)  # TODO: Explain what this is
    u = q / (area * dt_log_mean)

    if u >= max_value:
        u = max_value
    elif u <= min_value:
        u = min_value

    return u * enabler
