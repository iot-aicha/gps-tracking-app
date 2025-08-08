import azure.functions as func
import uuid
import json
import os
import logging
from azure.storage.blob import BlobServiceClient
from shapely.geometry import Point, Polygon
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Email imports
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_geofence_alert(lat: float, lon: float, distance: float, device_id: str = "unknown"):
    """Sends email when asset enters geofence"""
    logger.info("❗ Attempting to send notification...")
    
    if "SendGridApiKey" not in os.environ:
        logger.warning("⚠️ SendGridApiKey not found - running locally? Using mock notification")
        logger.info(f"📧 MOCK EMAIL: Geofence alert for device {device_id} at ({lat}, {lon})")
        return
        
    message = Mail(
        from_email=os.getenv("SenderEmail", "notifications@yourdomain.com"),
        to_emails=os.getenv("AlertRecipientEmail"),
        subject=f"🚨 {device_id} Entered Geofence",
        plain_text_content=(
            f"Device {device_id} has entered the geofence area!\n\n"
            f"📍 Coordinates: {lat}, {lon}\n"
            f"📏 Distance from boundary: {abs(distance):.2f}m\n"
            f"⏱ Time: {datetime.utcnow().isoformat()}"
        )
    )
    try:
        sg = SendGridAPIClient(os.environ["SendGridApiKey"])
        response = sg.send(message)
        logger.info(f"✅ Notification sent (Status: {response.status_code})")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {str(e)}")

def get_or_create_container(name):
    """Helper to get or create blob container"""
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ['STORAGE_CONNECTION_STRING']
    )
    try:
        return blob_service_client.get_container_client(name)
    except Exception:
        return blob_service_client.create_container(name)

def point_in_geofence(lat, lon, geofence_data):
    """Check if point is inside geofence polygon"""
    try:
        point = Point(lon, lat)
        logger.info(f"Checking point: lat={lat}, lon={lon}")
        
        # Handle different geofence formats
        if geofence_data['type'] == 'FeatureCollection':
            geometry = geofence_data['features'][0]['geometry']
        else:
            geometry = geofence_data
            
        if geometry['type'] == 'Polygon':
            polygon = Polygon(geometry['coordinates'][0])
            if polygon.contains(point):
                distance = point.distance(polygon.boundary) * 111000  # Convert to meters
                return -999 if distance > 50 else -distance
            return 999 if point.distance(polygon.boundary) * 111000 > 50 else point.distance(polygon.boundary) * 111000
    except Exception as e:
        logger.error(f"Geofence error: {e}")
        return 999

def process_geofence_event(event_body, device_id="test-device"):
    """Process geofence check for coordinates"""
    logger.info(f"Processing event from {device_id}: {event_body}")
    
    # Extract GPS coordinates
    gps_data = event_body.get('gps', event_body)
    lat = gps_data.get('lat') or gps_data.get('latitude')
    lon = gps_data.get('lon') or gps_data.get('longitude')
    
    if None in (lat, lon):
        logger.error("GPS coordinates not found in event data")
        return
        
    logger.info(f"GPS coordinates: lat={lat}, lon={lon}")
    
    # Get geofence data
    blob_client = BlobServiceClient.from_connection_string(
    os.environ['STORAGE_CONNECTION_STRING']
).get_blob_client(
    container=os.environ.get('GEOFENCE_CONTAINER', 'geofences'),
    blob=os.environ.get('GEOFENCE_BLOB_NAME', 'geofence/geofence.json')
)
    
    logger.info("Downloading geofence data...")
    geofence_data = json.loads(blob_client.download_blob().readall())
    
    # Check geofence status
    distance = point_in_geofence(lat, lon, geofence_data)
    
    if distance <= 0:  # Inside geofence
        logger.info(f'🟢 Point is {abs(distance):.2f}m INSIDE geofence')
        send_geofence_alert(lat, lon, distance, device_id)
    else:
        logger.info(f'🔴 Point is {distance:.2f}m OUTSIDE geofence')

# Event Hub Trigger for GPS Data Storage
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="iothub-ehub-gps-sensor-56808134-3dc3eb7f05",
    connection="EventHubConnectionString",
    consumer_group="$Default",
    cardinality=func.Cardinality.ONE
) 
def eventhub_trigger(azeventhub: func.EventHubEvent):
    try:
        logger.info("=== EventHub Trigger Started ===")
        event_body = json.loads(azeventhub.get_body().decode('utf-8'))
        
        # Get device ID
        device_id = (
            getattr(azeventhub, 'iothub_metadata', {}).get('connection-device-id') 
            or event_body.get('deviceId', 'unknown-device'))
        
        logger.info(f"Processing data from device: {device_id}")
        
        # Store data in blob storage
        container_client = get_or_create_container('gps-data')
        blob_name = f"{device_id}/{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"
        
        data_to_store = {
    'device_id': device_id,
    'timestamp': getattr(azeventhub, 'iothub_metadata', {}).get('enqueuedtime') or datetime.utcnow().isoformat(),
    'gps': event_body.get('gps', event_body),
    'date': datetime.utcnow().strftime('%Y-%m-%d')  # Add date for easier filtering
}
        
        container_client.get_blob_client(blob_name).upload_blob(
            json.dumps(data_to_store, default=str), 
            overwrite=True
        )
        
        logger.info(f"Successfully stored GPS data for device {device_id}")
        
    except Exception as e:
        logger.error(f"Event processing failed: {e}")
        
        
        
@app.route(route="list_gps_dates", auth_level=func.AuthLevel.FUNCTION)
def list_gps_dates(req: func.HttpRequest) -> func.HttpResponse:
    """Returns available dates with GPS data"""
    try:
        container_client = get_or_create_container('gps-data')
        
        # List all blobs and extract unique dates
        dates = set()
        for blob in container_client.list_blobs():
            # Extract date from path like "device_id/2023/12/25/..."
            parts = blob.name.split('/')
            if len(parts) >= 4:
                dates.add(f"{parts[1]}-{parts[2]}-{parts[3]}")
        
        return func.HttpResponse(
            json.dumps(sorted(list(dates))),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    
    

# Geofence Trigger - Runs in separate consumer group
@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="iothub-ehub-gps-sensor-56808134-3dc3eb7f05",
    connection="EventHubConnectionString",
    consumer_group="geofence",  # Different consumer group
    cardinality=func.Cardinality.ONE
)
def geofence_trigger(azeventhub: func.EventHubEvent):
    try:
        logger.info("=== Geofence Trigger Started ===")
        event_body = json.loads(azeventhub.get_body().decode('utf-8'))
        
        # Get device ID
        device_id = (
            getattr(azeventhub, 'iothub_metadata', {}).get('connection-device-id') 
            or event_body.get('deviceId', 'unknown-device'))
        
        # Process geofence check
        process_geofence_event(event_body, device_id)
            
    except Exception as e:
        logger.error(f"Geofence processing failed: {e}")

# Test endpoint
@app.route(route="test_geofence", auth_level=func.AuthLevel.ANONYMOUS)
def test_geofence(req: func.HttpRequest) -> func.HttpResponse:
    """Test endpoint for manual geofence checks"""
    try:
        # Get test coordinates from query params or use defaults
        lat = float(req.params.get('lat', 36.5273))
        lon = float(req.params.get('lon', 5.86945))
        
        test_event = {"gps": {"lat": lat, "lon": lon}}
        process_geofence_event(test_event, "test-device")
        
        return func.HttpResponse(
            f"Test geofence check completed for coordinates: {lat}, {lon}",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)