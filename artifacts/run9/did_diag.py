"""Diagnose the mvp-scope DiD non-monotonicity. Read-only against the warehouse."""
import sys
sys.path.insert(0, '/opt/s2fn/src')
import pandas as pd
from config import EVENT, ICEBERG, QUALITY
from session import get_sedona

pd.set_option('display.width', 200)
CAT = ICEBERG['catalog']
sedona = get_sedona('did_mvp_diag')
sedona.sparkContext.setLogLevel('ERROR')

print('== schemas ==')
for t in ('fields', 'field_ndvi', 'scenes', 'wind_zones'):
    print(t, [f.name for f in sedona.table(f'{CAT}.crop.{t}').schema.fields])

print('== dup checks ==')
print(sedona.sql(f"SELECT COUNT(*) AS rows, COUNT(DISTINCT field_id) AS ids FROM {CAT}.crop.fields").toPandas())
print(sedona.sql(f"""
  SELECT COUNT(*) AS rows, COUNT(DISTINCT field_id, date) AS field_dates
  FROM {CAT}.crop.field_ndvi
  WHERE date IN (DATE'{EVENT['pre_date']}', DATE'{EVENT['post_date']}')
""").toPandas())

print('== per-tile usable fraction on the event pair ==')
print(sedona.sql(f"""
  SELECT mgrs_tile, date, COUNT(*) AS field_obs,
         ROUND(AVG(CASE WHEN mean_ndvi IS NOT NULL
                         AND valid_frac >= {QUALITY['valid_frac_min']} THEN 1.0 ELSE 0.0 END), 3) AS usable_frac
  FROM {CAT}.crop.field_ndvi
  WHERE date IN (DATE'{EVENT['pre_date']}', DATE'{EVENT['post_date']}')
  GROUP BY mgrs_tile, date ORDER BY mgrs_tile, date
""").toPandas())

print('== frame (notebook cell 2 + lon) ==')
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

print('== attrition by band (cell 7 shape) ==')
att = frame.groupby('wind').agg(corn_fields=('usable', 'size'), cloud_ok=('usable', 'sum'))
att['dropped_pct'] = (100 * (1 - att.cloud_ok / att.corn_fields)).round(1)
print(att)

print('== raw delta + baseline by band (usable only) ==')
print(frame[frame.usable].groupby('wind').agg(
    n=('delta', 'size'), mean_pre=('pre', 'mean'), mean_post=('post', 'mean'),
    mean_delta=('delta', 'mean'), mean_lat=('lat', 'mean'), mean_lon=('lon', 'mean'),
    lon_p10=('lon', lambda s: s.quantile(.1)), lon_p90=('lon', lambda s: s.quantile(.9))).round(4))

print('== DiD (cell 4) ==')
band = float(EVENT['control_lat_band_deg'])
controls = frame[frame.usable & (frame.wind == 0)][['lat', 'lon', 'delta']].sort_values('lat').reset_index(drop=True)
treated = frame[frame.usable & (frame.wind > 0)].copy()
def control_mean(lat):
    sel = controls[(controls.lat >= lat - band) & (controls.lat <= lat + band)]
    return sel.delta.mean() if len(sel) >= 5 else None
treated['ctrl_delta'] = treated.lat.map(control_mean)
treated = treated.dropna(subset=['ctrl_delta'])
treated['did'] = treated.delta - treated.ctrl_delta
res = (treated.groupby('wind')
       .agg(fields=('did', 'size'), mean_delta=('delta', 'mean'),
            mean_ctrl=('ctrl_delta', 'mean'), did=('did', 'mean'),
            did_se=('did', lambda s: s.std() / len(s) ** .5)).round(4))
print(res)

print('== control pool ==')
print('controls usable:', len(controls), 'lon p10/p50/p90:',
      controls.lon.quantile([.1, .5, .9]).round(3).tolist())

print('== DiD by band x lon tercile of treated (drought-longitude probe) ==')
treated['lon_terc'] = pd.qcut(treated.lon, 3, labels=['west', 'mid', 'east'])
print(treated.groupby(['wind', 'lon_terc'], observed=True)
      .agg(n=('did', 'size'), did=('did', 'mean')).round(4))

print('== DiD at stricter validity (vf >= 0.8, diagnostic only) ==')
strict = frame[frame.pre.notna() & frame.post.notna() & (frame.pre_vf >= .8) & (frame.post_vf >= .8)]
sc = strict[strict.wind == 0][['lat', 'delta']].sort_values('lat').reset_index(drop=True)
st = strict[strict.wind > 0].copy()
def sc_mean(lat):
    sel = sc[(sc.lat >= lat - band) & (sc.lat <= lat + band)]
    return sel.delta.mean() if len(sel) >= 5 else None
st['ctrl_delta'] = st.lat.map(sc_mean)
st = st.dropna(subset=['ctrl_delta'])
st['did'] = st.delta - st.ctrl_delta
print(st.groupby('wind').agg(n=('did', 'size'), did=('did', 'mean')).round(4))
