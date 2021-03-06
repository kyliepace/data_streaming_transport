"""Defines trends calculations for stations"""
import logging
import faust
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    """station from kafka connect"""
    direction_id: str
    stop_id: int
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    """station with subset of kafka connect station plus line field"""
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#  places it into a new topic with only the necessary information.
app = faust.App("stream-stations", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic that Kafka Connect outputs to
topic = app.topic("connect-org.chicago.cta.stations", value_type=Station)

# Define the output Kafka Topic
out_topic = app.topic("faust.chicago.cta.stations", partitions=1, value_type=TransformedStation)
# Define a Faust Table
table = app.Table(
    "faust.chicago.cta.stations.table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

def add_line(station):
    """ add a line string property """
    line = 'undefined'
    if station.red:
        line = 'red'
    elif station.blue:
        line = 'blue'
    elif station.green:
        line = 'green'
    station.line = line
    return station


@app.agent(topic)
async def transformevent(stations):
    """transform input `Station` records into `TransformedStation` records"""
    stations.add_processor(add_line)
    async for station in stations:
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station.line
        )
        table["station_id"] = station.station_id
        table["station_name"] = station.station_name
        table["order"] = station.order
        table["line"] = station.line
        #
        # send the data to the topic you created above
        #
        await out_topic.send( value=transformed_station)


if __name__ == "__main__":
    app.main()
