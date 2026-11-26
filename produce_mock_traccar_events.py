import argparse
import base64
import bisect
import json
import math
import random
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

TRACCAR_WEB_URL = "http://localhost:8082"
TRACCAR_OSMAND_URL = "http://localhost:5055"
TRACCAR_USER = "admin"
TRACCAR_PASS = "admin"
EVENT_INTERVAL_SEC = 5

DEFAULT_ROUTE = [
    (41.0082, 28.9784),
    (41.0125, 28.9100),
    (41.0210, 28.8200),
    (41.0450, 28.6800),
    (41.0720, 28.5100),
    (41.1050, 28.2500),
    (41.1320, 28.0100),
    (41.1592, 27.8000),
]

VEHICLES = [
    {"uniqueId": "TRUCK-101", "name": "Logistics Truck 101", "start_ratio": 0.00},
    {"uniqueId": "VAN-202",   "name": "Delivery Van 202",    "start_ratio": 0.25},
    {"uniqueId": "CAR-303",   "name": "Field Service 303",   "start_ratio": 0.50},
]


def register_devices_in_traccar():
    """Ensures simulated devices exist in Traccar so port 5055 accepts their pings."""
    auth_str = base64.b64encode(f"{TRACCAR_USER}:{TRACCAR_PASS}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_str}",
        "Content-Type": "application/json",
    }

    try:
        req = urllib.request.Request(f"{TRACCAR_WEB_URL}/api/devices", headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            existing = {d["uniqueId"] for d in json.loads(resp.read().decode())}
    except Exception as e:
        print(f"[WARN] Could not query Traccar API ({e}). Make sure Traccar is running on :8082.")
        return

    for v in VEHICLES:
        if v["uniqueId"] not in existing:
            payload = json.dumps({"name": v["name"], "uniqueId": v["uniqueId"]}).encode()
            post_req = urllib.request.Request(
                f"{TRACCAR_WEB_URL}/api/devices", data=payload, headers=headers, method="POST"
            )
            try:
                urllib.request.urlopen(post_req, timeout=5)
                print(f"[SETUP] Registered device in Traccar: {v['uniqueId']}")
            except Exception as e:
                print(f"[WARN] Failed to register {v['uniqueId']}: {e}")


def load_route(filepath):
    """Loads waypoints from either a .gpx (XML) or .geojson file."""
    if filepath.lower().endswith(".gpx"):
        root = ET.parse(filepath).getroot()
        # Check track points (trkpt) first, then route points (rtept), then waypoints (wpt)
        for target_tag in ("trkpt", "rtept", "wpt"):
            coords = [
                (float(elem.attrib["lat"]), float(elem.attrib["lon"]))
                for elem in root.iter()
                if elem.tag.split("}")[-1] == target_tag
            ]
            if coords:
                print(f"[SETUP] Loaded {len(coords)} points from GPX file: {filepath}")
                return coords

    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        coords = []
        for feature in data.get("features", []):
            geom = feature.get("geometry", {})
            if geom.get("type") == "LineString":
                coords.extend([(lat, lon) for lon, lat in geom["coordinates"]])
            elif geom.get("type") == "MultiLineString":
                for line in geom["coordinates"]:
                    coords.extend([(lat, lon) for lon, lat in line])
        if coords:
            print(f"[SETUP] Loaded {len(coords)} points from GeoJSON file: {filepath}")
            return coords

    print(f"[WARN] No coordinates found in {filepath}. Using default route.")
    return DEFAULT_ROUTE


def haversine_m(lat1, lon1, lat2, lon2):
    """Calculates distance in meters between two GPS coordinates."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_route_index(route):
    """Precomputes cumulative distance along the route for accurate speed simulation."""
    clean_route = [route[0]]
    cum_dist = [0.0]
    for pt in route[1:]:
        d = haversine_m(clean_route[-1][0], clean_route[-1][1], pt[0], pt[1])
        if d >= 0.5:  # Skip duplicate points
            clean_route.append(pt)
            cum_dist.append(cum_dist[-1] + d)
    return clean_route, cum_dist


def position_at_distance(route, cum_dist, dist_m):
    """Finds exact (lat, lon, bearing) after traveling dist_m meters along the route."""
    total_m = cum_dist[-1]
    target = dist_m % total_m
    idx = min(bisect.bisect_right(cum_dist, target) - 1, len(route) - 2)

    seg_start = cum_dist[idx]
    seg_len = cum_dist[idx + 1] - seg_start
    frac = (target - seg_start) / seg_len if seg_len > 0 else 0.0

    lat1, lon1 = route[idx]
    lat2, lon2 = route[idx + 1]
    lat = round(lat1 + (lat2 - lat1) * frac, 6)
    lon = round(lon1 + (lon2 - lon1) * frac, 6)
    bearing = round(math.degrees(math.atan2(lon2 - lon1, lat2 - lat1)) % 360, 1)
    return lat, lon, bearing


def send_osmand_ping(device_id, lat, lon, speed_kmh, bearing):
    # OsmAnd protocol expects speed in knots (1 knot = 1.852 km/h)
    speed_knots = round(speed_kmh / 1.852, 2)
    params = urllib.parse.urlencode({
        "id": device_id,
        "lat": lat,
        "lon": lon,
        "speed": speed_knots,
        "bearing": bearing,
        "timestamp": int(time.time()),
    })
    with urllib.request.urlopen(f"{TRACCAR_OSMAND_URL}/?{params}", timeout=5) as resp:
        return resp.status


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send simulated GPS events to Traccar server.")
    parser.add_argument("--route", help="Path to a .gpx or .geojson route file", default=None)
    args = parser.parse_args()

    raw_route = load_route(args.route) if args.route else DEFAULT_ROUTE
    route, cum_dist = build_route_index(raw_route)
    total_length_km = round(cum_dist[-1] / 1000, 2)

    register_devices_in_traccar()

    # Stagger vehicles along the total length of your GPX track
    for v in VEHICLES:
        v["dist_m"] = cum_dist[-1] * v["start_ratio"]

    print(f"Route length: {total_length_km} km | Sending 1 event every {EVENT_INTERVAL_SEC}s...")
    tick = 0
    vehicle_step_sec = EVENT_INTERVAL_SEC * len(VEHICLES)

    try:
        while True:
            vehicle = VEHICLES[tick % len(VEHICLES)]
            speed_kmh = random.randint(85, 112) if random.random() < 0.30 else random.randint(45, 75)

            lat, lon, bearing = position_at_distance(route, cum_dist, vehicle["dist_m"])
            vehicle["dist_m"] += (speed_kmh / 3.6) * vehicle_step_sec

            try:
                status = send_osmand_ping(vehicle["uniqueId"], lat, lon, speed_kmh, bearing)
                print(
                    f"[TRACCAR HTTP {status}] {vehicle['uniqueId']} | "
                    f"({lat}, {lon}) | {speed_kmh} km/h | Bearing: {bearing}°"
                )
            except Exception as e:
                print(f"[ERROR] Failed to reach Traccar on :5055 -> {e}")

            tick += 1
            time.sleep(EVENT_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nStopped mock Traccar producer.")