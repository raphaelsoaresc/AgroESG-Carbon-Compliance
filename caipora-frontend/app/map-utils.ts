import L from "leaflet";

if (typeof window !== "undefined") {
  // @ts-ignore
  delete L.Icon.Default.prototype._getIconUrl;
  L.Icon.Default.mergeOptions({
    iconRetinaUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon-2x.png",
    iconUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon.png",
    shadowUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-shadow.png",
  });
}

export const ensureIterable = (geo: any) => {
  if (!geo) return null;
  try {
    const parsed = typeof geo === 'string' ? JSON.parse(geo) : geo;
    const validTypes = ["Polygon", "MultiPolygon", "GeometryCollection", "FeatureCollection", "Feature"];
    return validTypes.includes(parsed?.type) ? parsed : null;
  } catch (e) {
    return null;
  }
};