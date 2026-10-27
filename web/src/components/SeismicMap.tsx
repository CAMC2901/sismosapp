import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import { SeismicEvent } from '../api/client';

interface SeismicMapProps {
  events: SeismicEvent[];
  selectedEventId?: string | null;
}

type MapStyleKey = 'voyager' | 'satellite' | 'terrain' | 'dark';

const MAP_STYLES: Record<
  MapStyleKey,
  { label: string; url: string; attribution: string; subdomains?: string; className?: string }
> = {
  voyager: {
    label: 'Geográfico',
    url: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    attribution: '&copy; OpenStreetMap &copy; CARTO | USGS Catalog',
    subdomains: 'abcd',
  },
  satellite: {
    label: 'Satélite',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: 'Tiles &copy; Esri — Source: Esri, USGS, NOAA',
  },
  terrain: {
    label: 'Relieve',
    url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; OpenStreetMap contributors | USGS Catalog',
  },
  dark: {
    label: 'Oscuro Contraste',
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; OpenStreetMap &copy; CARTO | USGS Catalog',
    subdomains: 'abcd',
    className: 'leaflet-tile-dark-contrast',
  },
};

function getMagnitudeColor(mag: number): string {
  if (mag >= 6.0) return '#ef4444'; // Rojo carmesí (fuerte/mayor)
  if (mag >= 5.0) return '#f97316'; // Naranja intenso (moderado)
  if (mag >= 3.5) return '#f59e0b'; // Ámbar cálido (ligero)
  return '#eab308'; // Amarillo sísmico (menor/micro)
}

function getMagnitudeRadius(mag: number): number {
  const clamped = Math.max(1.8, Math.min(mag, 9.0));
  return Math.round(clamped * 2.5 + 2);
}

export const SeismicMap: React.FC<SeismicMapProps> = ({ events, selectedEventId }) => {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapInstanceRef = useRef<L.Map | null>(null);
  const tileLayerRef = useRef<L.TileLayer | null>(null);
  const layerGroupRef = useRef<L.LayerGroup | null>(null);
  const [mapStyle, setMapStyle] = useState<MapStyleKey>('voyager');

  // Inicialización del mapa + ResizeObserver para evitar bloques vacíos
  useEffect(() => {
    const container = mapContainerRef.current;
    if (!container || mapInstanceRef.current) return;

    const map = L.map(container, {
      center: [15.0, -25.0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 13,
      worldCopyJump: true,
      preferCanvas: true,
    });

    const initialCfg = MAP_STYLES.voyager;
    const tileLayer = L.tileLayer(initialCfg.url, {
      attribution: initialCfg.attribution,
      subdomains: initialCfg.subdomains || 'abc',
      maxZoom: 19,
      className: initialCfg.className || '',
    }).addTo(map);

    const layerGroup = L.layerGroup().addTo(map);
    mapInstanceRef.current = map;
    tileLayerRef.current = tileLayer;
    layerGroupRef.current = layerGroup;

    // Forzar recálculo del tamaño tras el montaje en el layout CSS Grid
    const t1 = window.setTimeout(() => map.invalidateSize(), 100);
    const t2 = window.setTimeout(() => map.invalidateSize(), 350);
    const t3 = window.setTimeout(() => map.invalidateSize(), 800);

    let observer: ResizeObserver | null = null;
    if (typeof ResizeObserver !== 'undefined') {
      observer = new ResizeObserver(() => {
        map.invalidateSize();
      });
      observer.observe(container);
    }

    return () => {
      clearTimeout(t1);
      clearTimeout(t2);
      clearTimeout(t3);
      if (observer) observer.disconnect();
      map.remove();
      mapInstanceRef.current = null;
      tileLayerRef.current = null;
      layerGroupRef.current = null;
    };
  }, []);

  // Cambio de estilo de capa base (Geográfico / Satélite / Relieve / Oscuro)
  useEffect(() => {
    const map = mapInstanceRef.current;
    if (!map) return;

    if (tileLayerRef.current) {
      map.removeLayer(tileLayerRef.current);
    }

    const cfg = MAP_STYLES[mapStyle];
    const nextLayer = L.tileLayer(cfg.url, {
      attribution: cfg.attribution,
      subdomains: cfg.subdomains || 'abc',
      maxZoom: 19,
      className: cfg.className || '',
    }).addTo(map);
    nextLayer.bringToBack();
    tileLayerRef.current = nextLayer;
    map.invalidateSize();
  }, [mapStyle]);

  // Renderizado de epicentros sísmicos ordenados de menor a mayor magnitud
  useEffect(() => {
    const map = mapInstanceRef.current;
    const group = layerGroupRef.current;
    if (!map || !group) return;

    group.clearLayers();

    // Ordenar de menor a mayor magnitud para que los sismos fuertes queden arriba
    const sortedEvents = [...events].sort((a, b) => a.mag - b.mag);

    sortedEvents.forEach((ev) => {
      if (typeof ev.lat !== 'number' || typeof ev.lon !== 'number') return;
      const color = getMagnitudeColor(ev.mag);
      const radius = getMagnitudeRadius(ev.mag);

      // Halo exterior para sismos significativos (M >= 5.0)
      if (ev.mag >= 5.0) {
        L.circleMarker([ev.lat, ev.lon], {
          radius: radius + 6,
          fillColor: color,
          color: color,
          weight: 1.5,
          opacity: 0.55,
          fillOpacity: 0.18,
          interactive: false,
        }).addTo(group);
      }

      const marker = L.circleMarker([ev.lat, ev.lon], {
        radius,
        fillColor: color,
        color: ev.tsunami === 1 ? '#38bdf8' : '#0f172a',
        weight: ev.tsunami === 1 ? 2.5 : 1.5,
        opacity: 0.98,
        fillOpacity: 0.85,
      });

      const dtFormatted = new Date(ev.time).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
      const popupHtml = `
        <div style="min-width: 225px; font-size: 12px; line-height: 1.45;">
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

  const handleResetGlobalView = () => {
    const map = mapInstanceRef.current;
    if (map) {
      map.setView([15.0, -25.0], 2, { animate: true });
    }
  };

  return (
    <div className="relative w-full h-[460px] rounded-2xl overflow-hidden border border-slate-800 shadow-inner bg-slate-950">
      <div ref={mapContainerRef} className="w-full h-full z-0" />

      {/* Barra Superior de Capas y Vista Global */}
      <div className="absolute top-3 right-3 z-[400] flex flex-wrap items-center gap-1.5 bg-slate-950/90 backdrop-blur-md border border-slate-800 rounded-xl p-1.5 shadow-lg">
        {(Object.keys(MAP_STYLES) as MapStyleKey[]).map((key) => (
          <button
            key={key}
            type="button"
            onClick={() => setMapStyle(key)}
            className={`px-2.5 py-1 rounded-lg text-[11px] font-semibold transition ${
              mapStyle === key
                ? 'bg-orange-600 text-white shadow'
                : 'text-slate-300 hover:bg-slate-800'
            }`}
          >
            {MAP_STYLES[key].label}
          </button>
        ))}
        <button
          type="button"
          onClick={handleResetGlobalView}
          className="px-2.5 py-1 rounded-lg text-[11px] font-semibold bg-slate-800 hover:bg-slate-700 text-orange-300 border border-slate-700 transition"
          title="Restablecer zoom global"
        >
          Vista Global
        </button>
      </div>

      {/* Leyenda de Magnitud */}
      <div className="absolute bottom-3 left-3 z-[400] bg-slate-950/90 backdrop-blur-md border border-slate-800 rounded-xl px-3 py-2 text-xs text-slate-200 flex flex-wrap items-center gap-3 shadow-lg">
        <span className="font-semibold text-white">Escala Magnitud:</span>
        <span className="flex items-center gap-1.5">
          <span className="w-2.5 h-2.5 rounded-full bg-yellow-400 border border-slate-900 inline-block" /> &lt; 3.5
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-full bg-amber-500 border border-slate-900 inline-block" /> 3.5–4.9
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3.5 h-3.5 rounded-full bg-orange-500 border border-slate-900 inline-block" /> 5.0–5.9
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-4 h-4 rounded-full bg-red-500 border border-slate-900 inline-block" /> 6.0+
        </span>
      </div>
    </div>
  );
};
