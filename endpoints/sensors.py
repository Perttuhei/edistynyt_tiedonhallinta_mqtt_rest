from fastapi import APIRouter
from sqlalchemy import text

from db import DW

router = APIRouter(
    prefix="/api/sensors",
    tags=["Sensors"]
)

# kaikki sensorit ja niiden id jotka ovat tallennettu tietokantaan
# haetaan myös boolean true/false onko sensorilta tallennettuja arvoja
@router.get("/all")
async def get_all_sensors(dw: DW):
    _query_str = ("""SELECT sensor, sensor_id, 
    CASE WHEN sensor_fact.sensor_key IS NOT NULL THEN 'true' ELSE 'false' END AS stored_data 
    FROM sensor_dim 
    LEFT JOIN sensor_fact ON sensor_dim.sensor_id = sensor_fact.sensor_key
    GROUP BY sensor_id, sensor, stored_data;""")
    _query = text(_query_str)
    rows = dw.execute(_query)
    data = rows.mappings().all()
    return {"data": data}

# halutun sensorin data halutulta päivältä tunneittain keskiarvo
@router.get("/{sensor_id}/{day}/{hour}")
async def get_sensor_data_hourly_avg_by_day(dw: DW, sensor_id: int, year: int, month: int, day: int):
    _query_str = ("""SELECT sensor, hour, AVG(value_int) AS avg_value_int, AVG(value_float) AS avg_value_float, value_str
    FROM sensor_fact
    JOIN sensor_dim ON sensor_fact.sensor_key = sensor_dim.sensor_id
    JOIN device_dim ON sensor_fact.device_key = device_dim.device_id
    JOIN date_dim ON sensor_fact.date_key = date_dim.date_id
    WHERE date_dim.year = :year AND date_dim.month = :month AND date_dim.day = :day AND sensor_dim.sensor_id = :sensor_id
    GROUP BY hour;""")
    _query = text(_query_str)
    rows = dw.execute(_query, {"sensor_id": sensor_id, "year": year, "month": month,"day": day})
    data = rows.mappings().all()
    return {"data": data}

# halutun sensorin data kuukauden ajalta viikon keskiarvo
@router.get("/{sensor_id}/{year}/{month}/{week}")
async def get_sensor_data_weekly_avg_by_month(dw: DW, sensor_id: int, year: int, month: int):
    _query_str = ("""SELECT sensor, week, AVG(ROUND(CASE WHEN value_int IS NOT NULL THEN value_int ELSE value_float END, 1)) AS avg_value 
    FROM sensor_fact 
    JOIN sensor_dim ON sensor_fact.sensor_key = sensor_dim.sensor_id 
    JOIN device_dim ON sensor_fact.device_key = device_dim.device_id 
    JOIN date_dim ON sensor_fact.date_key = date_dim.date_id
    WHERE date_dim.year = :year AND date_dim.month = :month AND sensor_dim.sensor_id = :sensor_id
    GROUP BY week;""")
    _query = text(_query_str)
    rows = dw.execute(_query, {"sensor_id": sensor_id, "year": year, "month": month})
    data = rows.mappings().all()
    return {"data": data}
