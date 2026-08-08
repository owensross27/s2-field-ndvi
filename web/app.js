/* s2-field-ndvi web map. No build step: MapLibre UMD + pmtiles IIFE from CDN.
   Quantization contract with 04_publish.py: NDVI [-0.2, 1.0] -> 0..200,
   drop [-0.4, +0.1] -> 0..200, 255 = masked. */
"use strict";

const MASKED = "#4a4f57";
const RAMPS = {
  // absolute NDVI: red -> yellow -> green
  ndvi: { stops: ["#a50026", "#f46d43", "#fee08b", "#a6d96a", "#1a9850"],
          lo: "-0.2", hi: "1.0" },
  // change: dark red (big loss) -> white (none) -> blue (gain)
  drop: { stops: ["#67001f", "#d6604d", "#f7f7f7", "#92c5de", "#2166ac"],
          lo: "-0.40", hi: "+0.10" },
};
const META = {
  drop_q: { ramp: "drop", title: "NDVI change, Aug 4 to Aug 19" },
  pre_q:  { ramp: "ndvi", title: "NDVI, 2020-08-04 (pre-derecho)" },
  post_q: { ramp: "ndvi", title: "NDVI, 2020-08-19 (post-derecho)" },
};
const CROPS = { 1: "corn", 5: "soybeans" };
const WIND = ["outside swath", "60-79 mph", "80-99 mph", "100+ mph"];

const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  container: "map",
  center: [-92.07, 42.08],
  zoom: 10,
  style: {
    version: 8,
    sources: {
      fields: { type: "vector", url: "pmtiles://fields.pmtiles" },
      county: { type: "geojson", data: "county.geojson" },
    },
    layers: [
      { id: "bg", type: "background", paint: { "background-color": "#0b0d10" } },
      { id: "county-fill", type: "fill", source: "county",
        paint: { "fill-color": "#12161c" } },
      { id: "fields", type: "fill", source: "fields", "source-layer": "fields",
        paint: { "fill-color": MASKED, "fill-opacity": 0.92 } },
      { id: "county-line", type: "line", source: "county",
        paint: { "line-color": "#5b6674", "line-width": 1.5 } },
    ],
  },
});

function colorExpr(prop, rampName) {
  const r = RAMPS[rampName].stops;
  // drop_q ramp spans [-0.40, +0.10] so zero change lands at q=160: pin the
  // neutral color there, not at the arithmetic middle, or "no change" reads blue
  const anchors = rampName === "drop" ? [0, 80, 160, 180, 200] : [0, 50, 100, 150, 200];
  return ["case",
    ["==", ["get", prop], 255], MASKED,
    ["interpolate", ["linear"], ["get", prop],
      anchors[0], r[0], anchors[1], r[1], anchors[2], r[2], anchors[3], r[3], anchors[4], r[4]],
  ];
}

function setMetric(prop) {
  const meta = META[prop];
  map.setPaintProperty("fields", "fill-color", colorExpr(prop, meta.ramp));
  document.querySelectorAll("#metrics button").forEach(
    (b) => b.classList.toggle("on", b.dataset.m === prop));
  document.getElementById("legend-title").textContent = meta.title;
  const ramp = RAMPS[meta.ramp];
  document.getElementById("swatches").innerHTML =
    ramp.stops.map((c) => `<span style="background:${c}"></span>`).join("");
  document.getElementById("lab-lo").textContent = ramp.lo;
  document.getElementById("lab-hi").textContent = ramp.hi;
}

document.getElementById("metrics").addEventListener("click", (e) => {
  if (e.target.dataset.m) setMetric(e.target.dataset.m);
});

const tip = document.getElementById("tip");
const unq = (q, lo, hi) => (q === 255 ? null : lo + (q / 200) * (hi - lo));
map.on("mousemove", "fields", (e) => {
  const f = e.features[0].properties;
  const pre = unq(f.pre_q, -0.2, 1.0), post = unq(f.post_q, -0.2, 1.0);
  const d = unq(f.drop_q, -0.4, 0.1);
  tip.style.display = "block";
  tip.style.left = e.point.x + 14 + "px";
  tip.style.top = e.point.y + 14 + "px";
  tip.innerHTML =
    `<b>${CROPS[f.crop] || "crop " + f.crop}</b> · ${WIND[f.wind]}<br>` +
    `pre ${pre == null ? "masked" : pre.toFixed(2)} · ` +
    `post ${post == null ? "masked" : post.toFixed(2)} · ` +
    `change ${d == null ? "masked" : d.toFixed(2)}`;
});
map.on("mouseleave", "fields", () => (tip.style.display = "none"));

// styledata fires as soon as the inline style is parsed; "load" proved flaky here
map.once("styledata", () => setMetric("drop_q"));
// the canvas can measure its container before layout settles; observe instead of
// hoping a one-shot resize lands after layout
new ResizeObserver(() => map.resize()).observe(document.getElementById("map"));
