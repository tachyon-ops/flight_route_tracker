# Provider API summaries

This document records the third-party ADS-B provider endpoints used by this project,
their request parameters, and the expected response shapes so future tooling or AI
knows where to look. It is intentionally minimal — consult each provider's docs
for full details and rate limits.

## adsb.fi (opendata.adsb.fi)
- Base: https://opendata.adsb.fi
- Endpoints used:
  - Area: `/api/v3/lat/{lat}/lon/{lon}/dist/{dist}` — returns JSON with `ac` list.
  - Hex: `/api/v2/hex/{hex}` — returns JSON with `ac` list.
  - Reg: `/api/v2/registration/{reg}` — returns JSON with `ac` list.
  - Flight/callsign: `/api/v2/callsign/{flight}` — returns JSON with `ac` list.
- Response notes: `ac` entries vary but often include keys like `Icao/Icao24/hex`,
  `Call/Callsign/flight`, `Reg/Registration/r`, `Lat/Latitude/lat`, `Long/Lng/lon`,
  `Alt/Altitude/Alt_baro`, `Spd/Speed/gs`, `Track/Heading/track`, `RSSI/rssi`.

## adsb.lol (api.adsb.lol)
- Base: https://api.adsb.lol
- Endpoints used are analogous to adsb.fi: `/v2/lat/{lat}/lon/{lon}/dist/{dist}`,
  `/v2/hex/{hex}`, `/v2/registration/{reg}`, `/v2/callsign/{flight}`.
- Response shape is usually `{"ac": [...]}` or similar.

## adsbexchange (public-api.adsbexchange.com)
- Base: https://public-api.adsbexchange.com
- Endpoints used (VirtualRadar compatibility):
  - Area: `/VirtualRadar/AircraftList.json?lat={lat}&lng={lon}&fDst={dist}`
  - Hex: `/VirtualRadar/AircraftList.json?icao24={hex}`
  - Reg: `/VirtualRadar/AircraftList.json?reg={reg}`
  - Flight: `/VirtualRadar/AircraftList.json?callsign={flight}`
- Response notes: Returns keys like `acList` or `ac` containing aircraft objects.
  Normalization is required to map provider-specific keys (Icao/Icao24/Reg/Call etc.)
  into the internal shape used by this project.
- Docs: https://www.adsbexchange.com/datasets/

## OpenSky Network (opensky-network.org)
- Base: https://opensky-network.org
- Endpoints used:
  - Area (bounding box): `/api/states/all?lamin={lamin}&lomin={lomin}&lamax={lamax}&lomax={lomax}`
  - Aircraft state by icao24: `/api/states/all?icao24={hex}`
  - Flights by aircraft (historic): `/api/flights/aircraft?icao24={hex}` (requires auth)
- Response notes: `states` is an array of arrays. Each state entry has fixed index
  positions (see OpenSky API docs). We map the indices to keys:
  - [0]=icao24, [1]=callsign, [5]=longitude, [6]=latitude, [7]=baro_altitude,
    [9]=velocity, [10]=true_track
- Docs: https://opensky-network.org/apidoc/index.html

## Notes for future AI / maintainers
- The internal codebase normalizes provider responses in `src/main.py` using
  `_normalize_adsbexchange_ac` and `_normalize_provider_response`.
- When adding new providers, add URL templates to `PROVIDERS` and an entry here
  documenting the expected parameters and response shape so the normalizer can
  be extended.
- Be mindful of rate limits: this project centralizes upstream throttling.
