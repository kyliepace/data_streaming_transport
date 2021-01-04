"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random

import requests
from confluent_kafka import avro

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    # rest_proxy_url = "http://localhost:8082"
    rest_proxy_url = "http://rest-proxy:8082/"

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_value.json")

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):

        super().__init__(
            "com.udacity.weather",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=3,
            num_replicas=3
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        #
        #
        # post a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/com.udacity.weather",
            headers=headers,
            data=json.dumps(
                {
                    "value_schema": Weather.value_schema,
                    "key_schema": Weather.key_schema,
                    "records": [
                        {
                            "value": {
                                "temperature": self.temp,
                                "status": self.status
                            }
                        }
                    ]
                }
            ),
        )
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
