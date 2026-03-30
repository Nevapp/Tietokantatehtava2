import requests
import pandas as pd
import io
from urllib.parse import quote
from database import get_connection

API_KEY = "2387efe836c710cf3e87c701abbb95def7a5b4ec345a73a66b7c4034830ce0b5"


def get_bbox(city):
    osm_url = f"https://nominatim.openstreetmap.org/search?q={quote(city)}&format=json"
    headers = {'User-Agent': 'OpenAQCityBBox'}

    response = requests.get(osm_url, headers=headers).json()

    if not response:
        return None

    osm_bbox = response[0]['boundingbox']
    min_lat, max_lat, min_lon, max_lon = osm_bbox
    openaq_bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    return openaq_bbox


def get_openaq_locations_by_bbox(_bbox):
    response = requests.get(
        f'https://api.openaq.org/v3/locations?limit=1000&page=1&order_by=id&sort_order=asc&bbox={_bbox}',
        headers={'X-API-Key': API_KEY})

    if response.status_code == 200:
        return response.json()['results']
    else:
        print("Virhe:", response.status_code)
        return []


def download_file_by_location(location_id, year, month, day):
    date_str = f"{year}{month:02d}{day:02d}"
    base_url = "https://openaq-data-archive.s3.amazonaws.com"
    key = f"records/csv.gz/locationid={location_id}/year={year}/month={month:02d}/location-{location_id}-{date_str}.csv.gz"
    full_url = f"{base_url}/{key}"

    response = requests.get(full_url)

    if response.status_code == 200:
        df = pd.read_csv(io.BytesIO(response.content), compression='gzip')

conn=get_connection()
cursor=conn.cursor()

for _, row in df.iterrows():
    cursor.execute("""
    INSERT INTO Mittaukset (sensoriID, arvo, aika)
    VALUES (%s,%s,%s)
    """,(row ["sensoriId"],row ["arvo"],row ["aika"]))
    conn.commit()
    cursor.close()
    conn.close()
        
    print("Tallennettu:", f"{location_id}-{date_str}.csv")
    else:
        print("Ei dataa, status:", response.status_code)


if __name__ == "__main__":
    bbox = get_bbox("New York City")
    print("BBOX:", bbox)

    locations = get_openaq_locations_by_bbox(bbox)
    print("Locations löytyi:", len(locations))

    if locations:
        first_location_id = locations[0]["id"]
        print("Haetaan data location_id:", first_location_id)
        download_file_by_location(first_location_id, 2023, 1, 15)
        for day in range(1, 31):
            download_file_by_locations(first_location_id, 2023, 1, day)
