"""Diag 2: (a) post-hoc lat+lon matched DiD, (b) Benton reproduction from mvp warehouse."""
import sys
sys.path.insert(0, '/opt/s2fn/src')
import numpy as np
import pandas as pd
from config import EVENT, ICEBERG, QUALITY
from session import get_sedona

pd.set_option('display.width', 200)
CAT = ICEBERG['catalog']
sedona = get_sedona('did_mvp_diag2')
sedona.sparkContext.setLogLevel('ERROR')

print('== counties schema ==')
print('counties', [f.name for f in sedona.table(f'{CAT}.crop.counties').schema.fields])

frame = sedona.sql(f"""
    WITH wide AS (
      SELECT field_id,
             MAX(CASE WHEN date = DATE'{EVENT['pre_date']}'  THEN mean_ndvi END) AS pre,
             MIN(CASE WHEN date = DATE'{EVENT['pre_date']}'  THEN valid_frac END) AS pre_vf,
             MAX(CASE WHEN date = DATE'{EVENT['post_date']}' THEN mean_ndvi END) AS post,
             MIN(CASE WHEN date = DATE'{EVENT['post_date']}' THEN valid_frac END) AS post_vf
      FROM {CAT}.crop.field_ndvi GROUP BY field_id
    )
    SELECT w.field_id, w.post - w.pre AS delta,
           w.pre, w.post, w.pre_vf, w.post_vf,
           ST_Y(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb))) AS lat,
           ST_X(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb))) AS lon,
           COALESCE(MAX(z.gust_class), 0) AS wind
    FROM wide w
    JOIN {CAT}.crop.fields f USING (field_id)
    LEFT JOIN {CAT}.crop.wind_zones z
      ON ST_Intersects(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb)),
                       ST_GeomFromWKB(z.wkb_4326))
    WHERE f.CDL2020 = 1
    GROUP BY w.field_id, w.pre, w.post, w.pre_vf, w.post_vf, f.geom_4326_wkb
""").toPandas()
vf_min = float(QUALITY['valid_frac_min'])
frame['usable'] = (frame.pre.notna() & frame.post.notna()
                   & (frame.pre_vf >= vf_min) & (frame.post_vf >= vf_min))
print('frame rows:', len(frame), 'usable:', int(frame.usable.sum()))

band = float(EVENT['control_lat_band_deg'])
ctrl = frame[frame.usable & (frame.wind == 0)][['lat', 'lon', 'delta']].sort_values('lat')
clat = ctrl.lat.to_numpy(); clon = ctrl.lon.to_numpy(); cdel = ctrl.delta.to_numpy()
treated = frame[frame.usable & (frame.wind > 0)].copy()

def matched_mean(lat, lon, lon_band):
    lo, hi = np.searchsorted(clat, (lat - band, lat + band))
    if lon_band is None:
        sel = cdel[lo:hi]
    else:
        m = np.abs(clon[lo:hi] - lon) <= lon_band
        sel = cdel[lo:hi][m]
    return sel.mean() if sel.size >= 5 else np.nan

for lon_band, label in ((None, 'lat only (pre-registered)'), (band, 'lat AND lon +/-0.15 (post-hoc)')):
    t = treated.copy()
    t['ctrl_delta'] = [matched_mean(la, lo, lon_band) for la, lo in zip(t.lat, t.lon)]
    t = t.dropna(subset=['ctrl_delta'])
    t['did'] = t.delta - t.ctrl_delta
    print(f'== DiD, controls matched on {label} ==')
    print(t.groupby('wind').agg(fields=('did', 'size'), did=('did', 'mean'),
                                did_se=('did', lambda s: s.std() / len(s) ** .5)).round(4))

print('== Benton county (19011) reproduction from mvp warehouse ==')
benton = sedona.sql(f"""
    SELECT c.wkb_4326 FROM {CAT}.crop.counties c WHERE c.GEOID = '19011'
""").toPandas()
if len(benton) == 0:
    print('GEOID lookup failed; counties table content:')
    print(sedona.sql(f'SELECT * FROM {CAT}.crop.counties LIMIT 3').toPandas())
else:
    from shapely import wkb as shwkb, contains_xy
    geom = shwkb.loads(bytes(benton.wkb_4326[0]))
    inb = contains_xy(geom, frame.lon.to_numpy(), frame.lat.to_numpy())
    bf = frame[inb]
    print('benton corn fields:', len(bf), 'usable:', int(bf.usable.sum()))
    bc = bf[bf.usable & (bf.wind == 0)][['lat', 'delta']].sort_values('lat')
    blat = bc.lat.to_numpy(); bdel = bc.delta.to_numpy()
    bt = bf[bf.usable & (bf.wind > 0)].copy()
    def bmean(lat):
        lo, hi = np.searchsorted(blat, (lat - band, lat + band))
        sel = bdel[lo:hi]
        return sel.mean() if sel.size >= 5 else np.nan
    bt['ctrl_delta'] = bt.lat.map(bmean)
    bt = bt.dropna(subset=['ctrl_delta'])
    bt['did'] = bt.delta - bt.ctrl_delta
    print(bt.groupby('wind').agg(fields=('did', 'size'), did=('did', 'mean'),
                                 did_se=('did', lambda s: s.std() / len(s) ** .5)).round(4))
    print('(committed demo-warehouse numbers: 527/-0.0144, 142/-0.0412, 116/-0.0561;')
    print(' demo controls were BENTON-ONLY so an exact match is not expected --')
    print(' this check validates data consistency, not identical control pools)')
