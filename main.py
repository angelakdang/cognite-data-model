import arrow
from cognite.client import CogniteClient

from auth import get_cognite_client
from utils import calculate_u_value, get_heat_exchanger_data


def handle(client: CogniteClient):
    # Get heat exchanger data as HeatExchanger dataclass instance from yaml
    heat_exchangers = get_heat_exchanger_data()

    # Define time period. Default is last 24 hours.
    end = arrow.utcnow()
    end_ms = end.timestamp() * 1000
    start_ms = end.shift(hours=-1).timestamp() * 1000

    for hx in heat_exchangers:
        # Get list of external ids of the timeseries we need to retrieve
        ts_xids = [
            hx.coolWatSupplyTemp,
            hx.coolWatReturnTemp,
            hx.processSupplyTemp,
            hx.processReturnTemp,
            hx.coolWatFlow,
        ]
        if hx.enabler:
            ts_xids.append(hx.enabler)

        # Modified from original code, which calls data over a long period of time
        # Intention is to run this as a Cognite function, so we will retrieve raw data from the last hour and average
        # TODO: handle case where there are large gaps in the retrieved data (ffill)
        # TODO: handle case where no data is retrieved (get_latest)
        data = client.time_series.data.retrieve(
            external_id=ts_xids, start=start_ms, end=end_ms
        )
        data_avg = {d.external_id: sum(d.value) / len(d.value) for d in data}
        u_value = calculate_u_value(
            cool_wat_supply_temp=data_avg[hx.coolWatSupplyTemp],
            cool_wat_return_temp=data_avg[hx.coolWatReturnTemp],
            process_supply_temp=data_avg[hx.processSupplyTemp],
            process_return_temp=data_avg[hx.processReturnTemp],
            cool_wat_flow=data_avg[hx.coolWatFlow],
            area=hx.designArea,
            enabler=data_avg.get(hx.enabler, 1),
        )
        print((hx.name, end_ms, u_value))
        # TODO: Write values back to CDF


# Test handle function
handle(get_cognite_client())
