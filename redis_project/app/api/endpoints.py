import pandas as pd
from fastapi import FastAPI, Query, UploadFile, File, APIRouter
import redis
import io
from fastapi.responses import ORJSONResponse

# Initialize FastAPI app
router = APIRouter()

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, password='admin')


# Read csv file and sort by sts column
# csv_data = pd.read_csv('/home/hexa/Downloads/raw_data.csv')
# csv_data.sort_values(by='sts', inplace=True)

# Function to store latest data of each device ID in Redis cache
def store_latest_data(csv_data):
    latest_data = {}
    for _, row in csv_data.iterrows():
        device_id = row['device_fk_id']
        latest_data[device_id] = {'time': row['sts'], 'location': (row['latitude'], row['longitude'])}
        # Store in Redis with device_id as key
        redis_client.set(device_id, str(latest_data[device_id]))


# Create API endpoints
@router.post("/upload/csv/")
async def upload_csv(file: UploadFile = File(...)):
    global csv_data
    contents = await file.read()
    csv_data = pd.read_csv(io.BytesIO(contents))
    csv_data.sort_values(by='sts', inplace=True)
    store_latest_data(csv_data)
    return {"message": "CSV file uploaded and data stored in Redis"}


# Endpoint 1: Get latest information of a device by device ID
@router.get('/api/device/latest_info/{device_id}')
async def get_latest_info(device_id: str):
    latest_info = redis_client.get(device_id)
    return latest_info


# Endpoint 2: Get start and end locations of a device by device ID
@router.get('/api/device/start_end_location/{device_id}')
async def get_start_end_location(device_id: str):
    start_location = None
    end_location = None
    for _, row in csv_data.iterrows():
        if row['device_fk_id'] == device_id:
            if start_location is None:
                start_location = (row['latitude'], row['longitude'])
            end_location = (row['latitude'], row['longitude'])
    return {'start_location': start_location, 'end_location': end_location}


# Endpoint 3: Get location points of a device within a time range by device ID, start time, and end time
@router.get('/api/device/location_points/{device_id}')
async def get_location_points(device_id: str, start_time: str = Query(None), end_time: str = Query(None)):
    location_points = []
    for _, row in csv_data.iterrows():
        if row['device_fk_id'] == device_id and start_time <= row['sts'] <= end_time:
            location_points.append(
                {'latitude': row['latitude'], 'longitude': row['longitude'], 'time_stamp': row['sts']})
    return location_points
