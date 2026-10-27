import React, { useEffect, useRef } from 'react';
import L from 'leaflet';
import { SeismicEvent } from '../api/client';

interface SeismicMapProps {
  events: SeismicEvent[];
  selectedEventId?: string | null;
}

function getMagnitudeColor(mag: number): string {
  if (mag >= 6.0) return '#ef4444'; // Rojo carmesí (fuerte/mayor)
  if (mag >= 5.0) return '#f97316'; // Naranja intenso (moderado)
  if (mag >= 3.5) return '#f59e0b'; // Ámbar cálido (ligero)
  return '#fbbf24'; // Amarillo sísmico (menor/micro)
}

function getMagnitudeRadius(mag: number): number {
  const clamped = Math.max(1.5, Math.min(mag, 9.0));
  return Math.round(clamped * 2.6 + 2);
}

export const SeismicMap: React.FC<SeismicMapProps> = ({ events, selectedEventId }) => {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapInstanceRef = useRef<L.Map | null>(null);
  const layerGroupRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    if (!mapContainerRef.current || mapInstanceRef.current) return;

    const map = L.map(mapContainerRef.current, {
      center: [15.0, -25.0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 12,
      worldCopyJump: true,
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO | USGS Catalog',
      subdomains: 'abcd',
      maxZoom: 19,
    }).addTo(map);

    const layerGroup = L.layerGroup().addTo(map);
    mapInstanceRef.current = map;
    layerGroupRef.current = layerGroup;

    return () => {
      map.remove();
      mapInstanceRef.current = null;
      layerGroupRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapInstanceRef.current;
    const group = layerGroupRef.current;
    if (!map || !group) return;

    group.clearLayers();

    events.forEach((ev) => {
      if (typeof ev.lat !== 'number' || typeof ev.lon !== 'number') return;
      const color = getMagnitudeColor(ev.mag);
      const radius = getMagnitudeRadius(ev.mag);

      const marker = L.circleMarker([ev.lat, ev.lon], {
        radius,
        fillColor: color,
        color: ev.tsunami === 1 ? '#38bdf8' : '#0f172a',
        weight: ev.tsunami === 1 ? 2.5 : 1.2,
        opacity: 0.95,
        fillOpacity: 0.78,
      });

      const dtFormatted = new Date(ev.time).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
      const popupHtml = `
        <div style="min-width: 220px; font-size: 12px; line-height: 1.45;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
            <span style="background:${color};color:#090d16;font-weight:800;padding:2px 8px;border-radius:6px;font-size:13px;">
              M ${ev.mag.toFixed(1)} (${ev.mag_type || 'ml'})
            </span>
            <span style="color:#94a3b8;font-family:monospace;">${ev.id}</span>
          </div>
          <div style="font-weight:700;color:#f8fafc;margin-bottom:4px;">${ev.place}</div>
          <div style="color:#cbd5e1;"><strong>Región:</strong> ${ev.region}</div>
          <div style="color:#cbd5e1;"><strong>Fecha:</strong> ${dtFormatted}</div>
          <div style="color:#cbd5e1;"><strong>Profundidad:</strong> ${ev.depth_km.toFixed(1)} km</div>
          <div style="color:#cbd5e1;"><strong>Coordenadas:</strong> [${ev.lat.toFixed(3)}, ${ev.lon.toFixed(3)}]</div>
          <div style="margin-top:4px;color:${ev.tsunami === 1 ? '#38bdf8' : '#94a3b8'};font-weight:600;">
            ${ev.tsunami === 1 ? '🌊 Alerta/Vigilancia de Tsunami Activa' : 'Sin alerta de tsunami'}
          </div>
          ${
            ev.url
              ? `<div style="margin-top:8px;"><a href="${ev.url}" target="_blank" rel="noopener noreferrer" style="color:#fb923c;text-decoration:underline;font-weight:600;">Ver boletín oficial USGS &rarr;</a></div>`
              : ''
          }
        </div>
      `;

      marker.bindPopup(popupHtml);
      marker.addTo(group);

      if (selectedEventId && ev.id === selectedEventId) {
        map.setView([ev.lat, ev.lon], 5, { animate: true });
        marker.openPopup();
      }
    });
  }, [events, selectedEventId]);

  return (
    <div className="relative w-full h-[430px] rounded-2xl overflow-hidden border border-slate-800 shadow-inner">
      <div ref={mapContainerRef} className="w-full h-full z-0" />
      <div className="absolute bottom-3 left-3 z-[400] bg-slate-950/90 backdrop-blur border border-slate-800 rounded-xl px-3 py-2 text-xs text-slate-300 flex flex-wrap items-center gap-3 shadow-lg">
        <span className="font-semibold text-slate-200">Escala Magnitud:</span>
        <span className="flex items-center gap-1.5">
          <span className="w-2.5 h-2.5 rounded-full bg-amber-300 inline-block" /> &lt; 3.5
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-full bg-amber-500 inline-block" /> 3.5–4.9
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3.5 h-3.5 rounded-full bg-orange-500 inline-block" /> 5.0–5.9
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-4 h-4 rounded-full bg-red-500 inline-block" /> 6.0+
        </span>
      </div>
    </div>
  );
};
