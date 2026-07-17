#!/usr/bin/env python3
"""GW3000 weather aggregator and FTPS uploader.

Usage: python main.py --settings config/settings.json --env config/.env

Receives GW3000 POST requests with weather data in form-encoded body, aggregates fixed
time windows, keeps compact 24h/7d history, and uploads JSON over FTPS.

Expected GW3000 request format:
    POST /data/report/ HTTP/1.1
    Content-Type: application/x-www-form-urlencoded
    stationtype=GW3000A_V1.2.2&tempf=XX.X&humidity=XX&winddir=XXX&windspeedmph=X.X&windgustmph=X.X&dailyrainin=XX.X&uv=X
"""

import argparse
import io
import json
import logging
import math
import os
import ssl
import threading
import time
from datetime import datetime, timedelta
from ftplib import FTP_TLS
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

LOG = logging.getLogger("weatherstation")

DEFAULT_SETTINGS = {
    "port": 8042,
    "interval_min": 15,
    "remote_file": "webcams/weather.json",
}

APP_CONFIG = DEFAULT_SETTINGS.copy()
FTP_CONFIG = {}


data_lock = threading.Lock()
current_interval_data = {
    "wind_speed": [],
    "wind_gust": [],
    "rain_rate": [],
    "daily_rain_total": [],
    "temp": [],
    "humidity": [],
    "wind_dir": [],
    "uv": [],
    "vpd": [],
    "solar_radiation": [],
}
last_wh65batt = None

# Tuples: (datetime, wind_avg, gust_max, rain_rate_avg, temp_avg, humidity_avg, wind_dir_avg, uv_max, uv_avg, vpd_avg, solar_radiation_avg)
raw_history_24h = []
history_7days = []

# Tuples: (datetime, wind_dir_deg) — one entry per incoming reading, kept for 1h
wind_dir_1h_raw = []


MPH_TO_MPS = 0.44704
INCH_TO_MM = 25.4
MPS_TO_KMH = 3.6
INHG_TO_KPA = 3.386389


def _wh65batt_status(value):
    if value is None:
        return "unknown"
    return "ok" if value == 0 else "low"


class _FTP_TLS_CustomHostname(FTP_TLS):
    """FTP_TLS subclass that uses a custom hostname for TLS SNI/verification."""

    tls_hostname = None

    def _with_tls_hostname(self, fn):
        if self.tls_hostname:
            real_host = self.host
            self.host = self.tls_hostname
            try:
                return fn()
            finally:
                self.host = real_host
        return fn()

    def auth(self):
        return self._with_tls_hostname(super().auth)

    def ntransfercmd(self, cmd, rest=None):
        return self._with_tls_hostname(
            lambda: super(_FTP_TLS_CustomHostname, self).ntransfercmd(cmd, rest)
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", default="config/settings.json")
    parser.add_argument("--env", default="config/.env")
    return parser.parse_args()


def load_settings(path):
    if not os.path.exists(path):
        LOG.warning("Settings file %s not found; using defaults", path)
        return DEFAULT_SETTINGS.copy()

    with open(path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    settings = DEFAULT_SETTINGS.copy()
    settings.update(loaded or {})
    return settings


def _env_required(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_ftp_config():
    host = _env_required("FTPS_HOST")
    user = _env_required("FTPS_USER")
    try:
        port = int(os.environ.get("FTPS_PORT", "21"))
    except ValueError:
        port = 21

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": os.environ.get("FTPS_PASSWORD"),
        "cafile": os.environ.get("FTPS_CAFILE"),
        "tls_hostname": os.environ.get("FTPS_TLS_HOSTNAME"),
    }


def _connect_ftps():
    ctx = ssl.create_default_context()
    cafile = FTP_CONFIG.get("cafile")
    if cafile:
        ctx.load_verify_locations(cafile)

    if FTP_CONFIG.get("tls_hostname"):
        ftp = _FTP_TLS_CustomHostname(context=ctx) if ctx else _FTP_TLS_CustomHostname()
        ftp.tls_hostname = FTP_CONFIG["tls_hostname"]
    else:
        ftp = FTP_TLS(context=ctx) if ctx else FTP_TLS()

    ftp.connect(FTP_CONFIG["host"], FTP_CONFIG["port"], timeout=30)
    ftp.login(FTP_CONFIG["user"], FTP_CONFIG.get("password"))
    ftp.prot_p()
    ftp.set_pasv(True)
    return ftp


def _ensure_remote_dir(ftp, remote_file):
    remote_dir = os.path.dirname(remote_file)
    if not remote_dir:
        return

    try:
        ftp.cwd("/")
    except Exception:
        pass

    for part in (p for p in remote_dir.split("/") if p):
        try:
            ftp.cwd(part)
        except Exception:
            try:
                ftp.mkd(part)
            except Exception:
                pass
            ftp.cwd(part)


def load_existing_data_or_init():
    """Load compact JSON from FTPS so restarts continue history smoothly."""
    global raw_history_24h, history_7days, wind_dir_1h_raw

    LOG.info("Loading existing history from FTPS (if available)")
    try:
        remote_file = APP_CONFIG["remote_file"]
        remote_name = os.path.basename(remote_file)
        with _connect_ftps() as ftp:
            _ensure_remote_dir(ftp, remote_file)
            flo = io.BytesIO()
            ftp.retrbinary(f"RETR {remote_name}", flo.write)
            flo.seek(0)
            existing_json = json.loads(flo.read().decode("utf-8"))

        series_24h = existing_json.get("series_24h", {})
        wind_dir_1h = existing_json.get("wind_dir_1h", {})
        timestamps = series_24h.get("ts", [])
        wind_avg_kmh = series_24h.get("wind_avg_kmh", [])
        wind_gust_kmh = series_24h.get("wind_gust_kmh", [])
        rain_rate_mmh = series_24h.get("rain_rate_mmh", [])
        temp_c = series_24h.get("temp_c", [])
        humidity_pct = series_24h.get("humidity_pct", [])
        wind_dir_deg = series_24h.get("wind_dir_deg", [])
        uv_index = series_24h.get("uv", [])
        uv_avg = series_24h.get("uv_avg", uv_index)
        vpd_kpa = series_24h.get("vpd_kpa", [])
        solar_radiation_wm2 = series_24h.get("solar_radiation_wm2", [])
        wind_dir_1h_ts = wind_dir_1h.get("ts", [])
        wind_dir_1h_vals = wind_dir_1h.get("wind_dir_deg", [])

        raw_history_24h = []
        for idx, ts in enumerate(timestamps):
            if (idx < len(wind_avg_kmh) and idx < len(wind_gust_kmh) and idx < len(rain_rate_mmh) and
                idx < len(temp_c) and idx < len(humidity_pct) and idx < len(wind_dir_deg) and idx < len(uv_index) and
                idx < len(uv_avg) and idx < len(vpd_kpa) and idx < len(solar_radiation_wm2)):
                # Parse ISO 8601 timestamp
                try:
                    dt_obj = datetime.fromisoformat(ts)
                except (ValueError, AttributeError):
                    # Fallback for older format
                    dt_obj = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                raw_history_24h.append((
                    dt_obj,
                    wind_avg_kmh[idx] / MPS_TO_KMH,
                    wind_gust_kmh[idx] / MPS_TO_KMH,
                    rain_rate_mmh[idx],
                    temp_c[idx],
                    humidity_pct[idx],
                    wind_dir_deg[idx],
                    uv_index[idx],
                    uv_avg[idx],
                    vpd_kpa[idx],
                    solar_radiation_wm2[idx],
                ))

        wind_dir_1h_raw = []
        for idx, ts in enumerate(wind_dir_1h_ts):
            if idx >= len(wind_dir_1h_vals):
                continue
            try:
                dt_obj = datetime.fromisoformat(ts)
            except (ValueError, TypeError):
                try:
                    dt_obj = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    continue
            wind_dir_1h_raw.append((dt_obj, wind_dir_1h_vals[idx]))

        history_7days = existing_json.get("rain_daily_7d", [])
        LOG.info(
            "Loaded %d interval(s), %d day entry(ies), and %d wind_dir_1h point(s)",
            len(raw_history_24h),
            len(history_7days),
            len(wind_dir_1h_raw),
        )
    except Exception as exc:
        LOG.warning("No prior history loaded (%s); starting fresh", exc)


def upload_json_to_ftp(data_dict):
    """Upload compact JSON payload to FTPS."""
    try:
        remote_file = APP_CONFIG["remote_file"]
        remote_name = os.path.basename(remote_file)
        payload = json.dumps(data_dict, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        with _connect_ftps() as ftp:
            _ensure_remote_dir(ftp, remote_file)
            tmp_name = f"{remote_name}.tmp"
            ftp.storbinary(f"STOR {tmp_name}", io.BytesIO(payload))
            try:
                ftp.rename(tmp_name, remote_name)
            except Exception:
                try:
                    ftp.delete(remote_name)
                except Exception:
                    pass
                ftp.rename(tmp_name, remote_name)
        LOG.info("Uploaded compact JSON at %s", datetime.now().strftime("%H:%M:%S"))
    except Exception as exc:
        LOG.exception("FTPS upload failed: %s", exc)


def update_history_and_cleanup(dt_obj, wind_avg, gust_max, rain_rate_avg, daily_rain_total, temp_avg, humidity_avg, wind_dir_avg, uv_max, uv_avg, vpd_avg, solar_radiation_avg):
    """Update 24h and 7d history views and upload current compact payload."""
    global raw_history_24h, history_7days, wind_dir_1h_raw

    # Anchor cleanup windows to the interval timestamp, not wall-clock now,
    # so the exported series keeps exact interval-aligned windows.
    dt_obj_cut = dt_obj.replace(second=0, microsecond=0)
    cutoff_24h = dt_obj_cut - timedelta(hours=24)
    cutoff_1h = dt_obj_cut - timedelta(hours=1)
    cutoff_7days = (dt_obj_cut - timedelta(days=7)).date()

    raw_history_24h.append((dt_obj, wind_avg, gust_max, rain_rate_avg, temp_avg, humidity_avg, wind_dir_avg, uv_max, uv_avg, vpd_avg, solar_radiation_avg))
    raw_history_24h = [item for item in raw_history_24h if item[0] >= cutoff_24h]
    raw_history_24h.sort(key=lambda x: x[0])

    # Internal wind unit is m/s; output exposes km/h for display consumers.
    timestamps_24h = [item[0].isoformat() for item in raw_history_24h]
    wind_avg_kmh = [round(item[1] * MPS_TO_KMH, 1) for item in raw_history_24h]
    wind_gust_kmh = [round(item[2] * MPS_TO_KMH, 1) for item in raw_history_24h]
    rain_rate_mmh = [round(item[3], 1) for item in raw_history_24h]
    temp_c = [round(item[4], 1) for item in raw_history_24h]
    humidity_pct = [int(round(item[5])) for item in raw_history_24h]
    wind_dir_deg = [int(round(item[6])) for item in raw_history_24h]
    uv_index = [round(item[7], 1) for item in raw_history_24h]
    uv_avg_index = [round(item[8], 1) for item in raw_history_24h]
    vpd_kpa = [round(item[9], 3) for item in raw_history_24h]
    solar_radiation_wm2 = [round(item[10], 1) for item in raw_history_24h]

    date_str = dt_obj.strftime("%Y-%m-%d")
    day_entry = next((item for item in history_7days if item["date"] == date_str), None)
    if day_entry:
        if daily_rain_total is not None:
            day_entry["rain_mm"] = round(daily_rain_total, 3)
    else:
        history_7days.append({
            "date": date_str,
            "rain_mm": round(daily_rain_total if daily_rain_total is not None else 0.0, 3),
        })

    history_7days = [
        item
        for item in history_7days
        if datetime.strptime(item["date"], "%Y-%m-%d").date() >= cutoff_7days
    ]
    history_7days.sort(key=lambda x: x["date"])

    with data_lock:
        wind_dir_1h_raw = [item for item in wind_dir_1h_raw if item[0] >= cutoff_1h]
        wind_dir_1h_snapshot = list(wind_dir_1h_raw)
        wh65batt_snapshot = last_wh65batt

    minute_buckets: dict = {}
    for ts, deg in wind_dir_1h_snapshot:
        minute_key = ts.replace(second=0, microsecond=0)
        minute_buckets.setdefault(minute_key, []).append(deg)

    wind_dir_1h_ts = []
    wind_dir_1h_vals = []
    for minute_key in sorted(minute_buckets):
        degs = minute_buckets[minute_key]
        sin_avg = sum(math.sin(math.radians(d)) for d in degs) / len(degs)
        cos_avg = sum(math.cos(math.radians(d)) for d in degs) / len(degs)
        wind_dir_1h_ts.append(minute_key.isoformat())
        wind_dir_1h_vals.append(round(math.degrees(math.atan2(sin_avg, cos_avg)) % 360, 1))

    output_data = {
        "version": 1,
        "status": {
            "wh65batt": _wh65batt_status(wh65batt_snapshot),
        },
        "series_24h": {
            "ts": timestamps_24h,
            "wind_avg_kmh": wind_avg_kmh,
            "wind_gust_kmh": wind_gust_kmh,
            "wind_dir_deg": wind_dir_deg,
            "temp_c": temp_c,
            "humidity_pct": humidity_pct,
            "uv": uv_index,
            "uv_avg": uv_avg_index,
            "vpd_kpa": vpd_kpa,
            "solar_radiation_wm2": solar_radiation_wm2,
            "rain_rate_mmh": rain_rate_mmh,
        },
        "rain_daily_7d": history_7days,
        "wind_dir_1h": {
            "ts": wind_dir_1h_ts,
            "wind_dir_deg": wind_dir_1h_vals,
        },
    }

    upload_json_to_ftp(output_data)


def aggregation_timer_worker():
    """Background thread: aggregate values for each configured interval."""
    global current_interval_data

    interval_min = max(1, int(APP_CONFIG["interval_min"]))
    interval_seconds = interval_min * 60
    now = datetime.now()
    seconds_into_hour = now.minute * 60 + now.second + now.microsecond / 1000000.0
    offset_into_interval = seconds_into_hour % interval_seconds

    # Align to wall-clock intervals (e.g., :00, :15, :30, :45 for 15-min intervals)
    if offset_into_interval < 5:
        # Within first 5 seconds of boundary, fire immediately
        seconds_to_wait = 0
    else:
        # Wait until next boundary
        seconds_to_wait = interval_seconds - offset_into_interval

    next_fire = datetime.now() + timedelta(seconds=seconds_to_wait)
    time.sleep(seconds_to_wait)

    while True:
        interval_end_time = datetime.now()
        next_fire += timedelta(seconds=interval_seconds)

        with data_lock:
            local_data = current_interval_data.copy()
            current_interval_data = {
                "wind_speed": [],
                "wind_gust": [],
                "rain_rate": [],
                "daily_rain_total": [],
                "temp": [],
                "humidity": [],
                "wind_dir": [],
                "uv": [],
                "vpd": [],
                "solar_radiation": [],
            }

        speeds = local_data["wind_speed"]
        wind_avg = round(sum(speeds) / len(speeds), 2) if speeds else 0.0

        gusts = local_data["wind_gust"]
        gust_max = round(max(gusts), 2) if gusts else 0.0

        rain_rates = local_data["rain_rate"]
        rain_rate_avg = round(sum(rain_rates) / len(rain_rates), 2) if rain_rates else 0.0

        daily_rain_totals = local_data["daily_rain_total"]
        daily_rain_total = daily_rain_totals[-1] if daily_rain_totals else None

        temps = local_data["temp"]
        temp_avg = round(sum(temps) / len(temps), 2) if temps else 0.0

        humidities = local_data["humidity"]
        humidity_avg = round(sum(humidities) / len(humidities), 2) if humidities else 0.0

        wind_dirs = local_data["wind_dir"]
        if wind_dirs:
            sin_avg = sum(math.sin(math.radians(d)) for d in wind_dirs) / len(wind_dirs)
            cos_avg = sum(math.cos(math.radians(d)) for d in wind_dirs) / len(wind_dirs)
            wind_dir_avg = round(math.degrees(math.atan2(sin_avg, cos_avg)) % 360, 1)
        else:
            wind_dir_avg = 0.0

        uvs = local_data["uv"]
        uv_max = round(max(uvs), 2) if uvs else 0.0
        uv_avg = round(sum(uvs) / len(uvs), 2) if uvs else 0.0

        vpds = local_data["vpd"]
        vpd_avg = round(sum(vpds) / len(vpds), 3) if vpds else 0.0

        solar_vals = local_data["solar_radiation"]
        solar_radiation_avg = round(sum(solar_vals) / len(solar_vals), 2) if solar_vals else 0.0

        update_history_and_cleanup(interval_end_time, wind_avg, gust_max, rain_rate_avg, daily_rain_total, temp_avg, humidity_avg, wind_dir_avg, uv_max, uv_avg, vpd_avg, solar_radiation_avg)
        sleep_secs = (next_fire - datetime.now()).total_seconds()
        time.sleep(max(0.0, sleep_secs))


class EcowittRequestHandler(BaseHTTPRequestHandler):
    @staticmethod
    def _first_value(parsed_payload, *keys):
        for key in keys:
            if key not in parsed_payload:
                continue

            raw = parsed_payload.get(key)
            if isinstance(raw, (list, tuple)):
                if not raw:
                    continue
                value = raw[0]
            else:
                value = raw

            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _to_celsius(temp_f):
        return (temp_f - 32.0) * 5.0 / 9.0

    def _ingest_payload(self, parsed_payload):
        global last_wh65batt
        logging.debug("Received payload: %s", parsed_payload)
        # Normalize GW3000 imperial units to internal units (m/s, mm, celsius, kPa).
        temp_f_raw = self._first_value(parsed_payload, "tempf")
        humidity_raw = self._first_value(parsed_payload, "humidity")
        vpd_raw = self._first_value(parsed_payload, "vpd")

        wind_dir_raw = self._first_value(parsed_payload, "winddir")

        wind_speed_mph_raw = self._first_value(parsed_payload, "windspeedmph")
        wind_gust_mph_raw = self._first_value(parsed_payload, "windgustmph")
        
        solar_radiation_raw = self._first_value(parsed_payload, "solarradiation")
        uv_raw = self._first_value(parsed_payload, "uv")

        rain_rate_in_raw = self._first_value(parsed_payload, "rainratein")
        rain_daily_in_raw = self._first_value(parsed_payload, "dailyrainin")
        wh65batt_raw = self._first_value(parsed_payload, "wh65batt")

        with data_lock:
            try:
                if wind_speed_mph_raw is not None:
                    wind_speed = float(wind_speed_mph_raw) * MPH_TO_MPS
                    current_interval_data["wind_speed"].append(wind_speed)

                if wind_gust_mph_raw is not None:
                    wind_gust = float(wind_gust_mph_raw) * MPH_TO_MPS
                    current_interval_data["wind_gust"].append(wind_gust)

                if rain_rate_in_raw is not None:
                    rain_rate_mmh = float(rain_rate_in_raw) * INCH_TO_MM
                    current_interval_data["rain_rate"].append(rain_rate_mmh)

                if rain_daily_in_raw is not None:
                    current_rain = float(rain_daily_in_raw) * INCH_TO_MM
                else:
                    current_rain = None

                if current_rain is not None:
                    current_interval_data["daily_rain_total"].append(current_rain)

                if wh65batt_raw is not None:
                    last_wh65batt = int(float(wh65batt_raw))

                if temp_f_raw is not None:
                    temp_c = self._to_celsius(float(temp_f_raw))
                    current_interval_data["temp"].append(temp_c)

                if humidity_raw:
                    current_interval_data["humidity"].append(float(humidity_raw))
                if wind_dir_raw:
                    _wind_dir_val = float(wind_dir_raw)
                    current_interval_data["wind_dir"].append(_wind_dir_val)
                    wind_dir_1h_raw.append((datetime.now(), _wind_dir_val))
                if uv_raw:
                    current_interval_data["uv"].append(float(uv_raw))
                if vpd_raw:
                    current_interval_data["vpd"].append(float(vpd_raw) * INHG_TO_KPA)
                if solar_radiation_raw:
                    current_interval_data["solar_radiation"].append(float(solar_radiation_raw))
            except (TypeError, ValueError):
                LOG.debug("Ignoring malformed payload: %s", parsed_payload)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length).decode("utf-8", errors="replace")
        content_type = (self.headers.get("Content-Type") or "").lower()

        if "application/json" in content_type:
            try:
                parsed_payload = json.loads(raw_body)
            except json.JSONDecodeError:
                parsed_payload = {}
        else:
            parsed_payload = parse_qs(raw_body)

        self._ingest_payload(parsed_payload)

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"SUCCESS")

    def log_message(self, _format, *_args):
        return


def run_server():
    load_existing_data_or_init()
    timer_thread = threading.Thread(target=aggregation_timer_worker, daemon=True)
    timer_thread.start()

    port = int(APP_CONFIG["port"])
    server_address = ("", port)
    httpd = HTTPServer(server_address, EcowittRequestHandler)
    LOG.info("Ecowitt server listening on port %s", port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Server interrupted, shutting down")
        httpd.server_close()


def main():
    global APP_CONFIG, FTP_CONFIG

    args = parse_args()

    if load_dotenv and os.path.exists(args.env):
        load_dotenv(args.env)

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

    APP_CONFIG = load_settings(args.settings)
    FTP_CONFIG = load_ftp_config()

    LOG.info(
        "Starting weatherstation with settings=%s and FTPS host=%s:%s",
        args.settings,
        FTP_CONFIG["host"],
        FTP_CONFIG["port"],
    )

    run_server()


if __name__ == "__main__":
    main()
