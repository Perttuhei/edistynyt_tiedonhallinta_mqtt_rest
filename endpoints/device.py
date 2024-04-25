from fastapi import APIRouter
from sqlalchemy import text

from db import DW

router = APIRouter(
    prefix="/api/device",
    tags=["Devices"]
)

# kaikki laitteet tietokannasta, nimi, id ja sensoreiden määrä laitteella
@router.get("/all")
async def get_all_devices(dw: DW):
    _query_str = ("SELECT device_dim.device_name, device_dim.device_id, COUNT(sensor_dim.device_id) AS number_of_sensors "
                  "FROM device_dim "
                  "LEFT JOIN sensor_dim ON device_dim.device_id = sensor_dim.device_id "
                  "GROUP BY device_dim.device_id")
    _query = text(_query_str)
    rows = dw.execute(_query)
    data = rows.mappings().all()
    return {"data": data}

# keskimääräinen value/unit viikottain
@router.get("/{year}/{month}/{week}")
async def get_device_avg_value_by_week(dw: DW, device_id: int, year: int, month: int):
    _query_str = ("""SELECT device_dim.device_name, week, AVG(ROUND(CASE WHEN value_int IS NOT NULL THEN value_int ELSE value_float END, 1)) AS avg_value, sensor_dim.unit 
                FROM sensor_fact 
                JOIN sensor_dim ON sensor_fact.sensor_key = sensor_dim.sensor_id 
                JOIN device_dim ON sensor_fact.device_key = device_dim.device_id 
                JOIN date_dim ON sensor_fact.date_key = date_dim.date_id
                WHERE date_dim.year = :year AND date_dim.month = :month AND device_dim.device_id = :device_id
                GROUP BY week;""")
    _query = text(_query_str)
    rows = dw.execute(_query, {"device_id": device_id, "year": year, "month": month})
    data = rows.mappings().all()
    return {"data": data}