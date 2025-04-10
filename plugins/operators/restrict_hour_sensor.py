"""

Operator to await restricted hours for which this dag succeeds

"""

import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException, AirflowSensorTimeout
from airflow.sensors.base import BaseSensorOperator

log = logging.getLogger(__name__)

class RestrictHourSensor(BaseSensorOperator):

    def __init__(self,
                valid_from_hour_utc,
                valid_to_hour_utc,
                mode="reschedule",
                timeout=timedelta(hours=24).total_seconds(),
                poke_interval=timedelta(minutes=15).total_seconds(),
                *args,
                **kwargs):
        super(RestrictHourSensor, self).__init__(
            mode=mode, 
            timeout=timeout,
            poke_interval=poke_interval,
            *args, 
            **kwargs
        )
        self.valid_from_hour_utc = valid_from_hour_utc
        self.valid_to_hour_utc = valid_to_hour_utc

    def poke(self, context):
        now = datetime.utcnow()
        # e.g. valid_from_hour_utc=1am and valid_to_hour_utc=5am
        if self.valid_from_hour_utc < self.valid_to_hour_utc:
            # just check if in range
            if now.hour >= self.valid_from_hour_utc and now.hour <= self.valid_to_hour_utc:
                return True
        
        # e.g. valid_from_hour_utc=22pm and valid_to_hour_utc=5am
        if self.valid_from_hour_utc > self.valid_to_hour_utc:
            # just check if in range
            if now.hour >= self.valid_from_hour_utc or now.hour <= self.valid_to_hour_utc:
                return True

        return False

    def execute(self, context):
        try:
            return super(RestrictHourSensor, self).execute(context)
        except AirflowSensorTimeout as e:
            raise AirflowFailException(str(e))