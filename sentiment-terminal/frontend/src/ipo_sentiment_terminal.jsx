// ═══════════════════════════════════════════════════════════════════════════
// IPO Sentiment Terminal — v6 frontend
//   • Light + Dark themes with persistence
//   • Glassmorphism cards (backdrop blur + translucent)
//   • Hover: smooth lift + soft teal glow shadow
//   • Sparklines inside metric cards
//   • Semicircular composite gauge + radar LLM compare
//   • Stacked-timeline FinBERT distribution
//   • Pulsing Live indicator + sidebar nav
//   • Elegant empty/unavailable data states
// ═══════════════════════════════════════════════════════════════════════════
import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";
import {
  Search, AlertTriangle, Activity, Newspaper, ChevronDown, Loader2, SearchX,
  ServerCrash, Sun, Moon, Bookmark, History, Star, Settings, Sparkles,
  TrendingUp, TrendingDown, Eye, EyeOff, Zap, Info, BarChart3,
  CircleDot, FileQuestion, Globe2,
} from "lucide-react";

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════
const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const USE_MOCK_FALLBACK = false;

// ═══════════════════════════════════════════════════════════════════════════
// THEME TOKENS
// ─────────────────────────────────────────────────────────────────────────
// All colors flow through `t` (the active theme). Components never reach
// for raw hex. To add a third theme: add another object here.
// ═══════════════════════════════════════════════════════════════════════════
const THEMES = {
  light: {
    name: "light",
    // Atmosphere
    bg: "#f1ebdb",                       // warm parchment cream
    bgGradient: "radial-gradient(circle at 20% 0%, #f7f1e3 0%, #ebe2cb 55%, #e2d6b7 100%)",
    // Glass surfaces
    surface: "rgba(255, 252, 245, 0.65)",
    surfaceSolid: "#fffcf5",
    surfaceAccent: "rgba(207, 235, 220, 0.55)",   // mint-tinted glass for highlight cards
    surfaceWarm: "rgba(247, 235, 210, 0.55)",     // warm-tinted glass
    surfaceHover: "rgba(255, 255, 250, 0.85)",
    // Borders & dividers
    border: "rgba(110, 95, 60, 0.15)",
    borderSoft: "rgba(110, 95, 60, 0.08)",
    borderStrong: "rgba(110, 95, 60, 0.28)",
    // Text
    textPrimary: "#1d2a25",
    textSecondary: "#4d5a52",
    textMuted: "#8a8472",
    textDim: "#b3a890",
    // Brand & semantic colors
    accent: "#16a34a",                   // primary action / positive
    accentSoft: "rgba(22, 163, 74, 0.12)",
    gold: "#a87420",
    goldSoft: "rgba(168, 116, 32, 0.12)",
    red: "#c0392b",
    redSoft: "rgba(192, 57, 43, 0.12)",
    blue: "#1e6fb8",
    teal: "#0e9488",
    tealGlow: "rgba(14, 148, 136, 0.28)",
    neutral: "#9aa39c",
    // Chart line tints
    chartGreen: "#16a34a",
    chartRed: "#c0392b",
    chartBlue: "#1e6fb8",
    // Hover glow
    glow: "0 12px 32px -8px rgba(14, 148, 136, 0.32), 0 4px 12px -2px rgba(14, 148, 136, 0.14)",
    shadowResting: "0 1px 3px rgba(60, 50, 30, 0.04), 0 4px 16px -4px rgba(60, 50, 30, 0.06)",
    // Background pattern (subtle topographic dots)
    bgPattern: `url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='60' height='60' viewBox='0 0 60 60'><g fill='%23a89870' fill-opacity='0.10'><circle cx='30' cy='30' r='1.2'/><circle cx='0' cy='0' r='1.2'/><circle cx='60' cy='0' r='1.2'/><circle cx='0' cy='60' r='1.2'/><circle cx='60' cy='60' r='1.2'/></g></svg>")`,
  },
  dark: {
    name: "dark",
    bg: "#0c1418",
    bgGradient: "radial-gradient(circle at 20% 0%, #14222a 0%, #0c1418 55%, #060a0d 100%)",
    surface: "rgba(22, 32, 38, 0.55)",
    surfaceSolid: "#172127",
    surfaceAccent: "rgba(14, 70, 64, 0.45)",
    surfaceWarm: "rgba(58, 42, 22, 0.40)",
    surfaceHover: "rgba(30, 42, 50, 0.75)",
    border: "rgba(180, 220, 210, 0.10)",
    borderSoft: "rgba(180, 220, 210, 0.05)",
    borderStrong: "rgba(180, 220, 210, 0.22)",
    textPrimary: "#e8eee9",
    textSecondary: "#a8b8b0",
    textMuted: "#6c7d76",
    textDim: "#4a5650",
    accent: "#34d399",
    accentSoft: "rgba(52, 211, 153, 0.16)",
    gold: "#d4a14a",
    goldSoft: "rgba(212, 161, 74, 0.18)",
    red: "#f87171",
    redSoft: "rgba(248, 113, 113, 0.16)",
    blue: "#60a5fa",
    teal: "#2dd4bf",
    tealGlow: "rgba(45, 212, 191, 0.45)",
    neutral: "#7a8a85",
    chartGreen: "#34d399",
    chartRed: "#f87171",
    chartBlue: "#60a5fa",
    glow: "0 12px 32px -8px rgba(45, 212, 191, 0.45), 0 4px 14px -2px rgba(45, 212, 191, 0.22)",
    shadowResting: "0 1px 3px rgba(0, 0, 0, 0.3), 0 4px 16px -4px rgba(0, 0, 0, 0.4)",
    bgPattern: `url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='60' height='60' viewBox='0 0 60 60'><g fill='%2374a8a0' fill-opacity='0.07'><circle cx='30' cy='30' r='1.2'/><circle cx='0' cy='0' r='1.2'/><circle cx='60' cy='0' r='1.2'/><circle cx='0' cy='60' r='1.2'/><circle cx='60' cy='60' r='1.2'/></g></svg>")`,
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// MOCK FALLBACK
// ═══════════════════════════════════════════════════════════════════════════
const MOCK_SEARCH_RESULTS = [
  { ipo_name: "Zomato Limited", listing_date: "2021-07-23", display_label: "Zomato Limited (Jul 2021)", listing_gain: 65.97 },
  { ipo_name: "Nykaa", listing_date: "2021-11-10", display_label: "Nykaa (Nov 2021)", listing_gain: 79.43 },
];

function generateMockSentiment(name, date) {
  const baseDate = new Date(date).getTime();
  const r = (s) => Math.abs(Math.sin(name.length * 7 + s * 13)) % 1;
  const avg_positive = 0.25 + r(1) * 0.5;
  const avg_negative = 0.10 + r(2) * 0.30;
  const avg_neutral = Math.max(0, 1 - avg_positive - avg_negative);
  const groq_score = -0.3 + r(3) * 1.1;
  const market_mood = -0.4 + r(4) * 1.4;
  const composite = 0.30 * groq_score + 0.25 * (avg_positive - avg_negative) + 0.25 * market_mood + 0.10 * (r(5) - 0.3);
  return {
    ipo_name: name, listing_date: date, _demo: true,
    article_count: 5, avg_positive, avg_negative, avg_neutral,
    dominant_sentiment: avg_positive > avg_negative ? "positive" : "negative",
    sentiment_momentum: r(6) - 0.3,
    groq_score, groq_summary: `[DEMO] Pre-IPO coverage on ${name} is mixed.`,
    llama_score: groq_score + (r(7) - 0.5) * 0.2, llama_summary: `[DEMO] Llama: similar verdict on ${name}.`,
    avg_flesch_score: 50 + r(8) * 30, market_mood_score: market_mood,
    macro_available: true, macro_score: -0.2 + r(9) * 0.6,
    macro_briefing: `[DEMO] Macro backdrop suggests moderate risk-on with neutral rates.`,
    composite_score: Math.max(-1, Math.min(1, composite)),
    nifty_price_t1: 22000 + r(10) * 2000,
    nifty_return_5d: -0.02 + r(11) * 0.06,
    nifty_price_series: Array.from({ length: 21 }, (_, i) => ({
      date: new Date(baseDate - (20 - i) * 86400000).toISOString().slice(0, 10),
      close: 22000 + Math.sin(i / 3) * 400 + r(12 + i) * 200,
    })),
    vix_t1: 12 + r(13) * 8, vix_avg_window: 14, vix_trend: r(14) - 0.5,
    sentiment_momentum_series: Array.from({ length: 7 }, (_, i) => ({
      date: new Date(baseDate - (6 - i) * 86400000).toISOString().slice(0, 10),
      momentum: -0.5 + r(15 + i) * 1.0,
    })),
    news_source: "gdelt_v2",
    articles: [
      { title: `${name} IPO oversubscribed 38x on final day`, sentiment_label: "positive", published_at: new Date(baseDate - 5 * 86400000).toISOString(), source: "Moneycontrol", finbert_positive: 0.82 },
      { title: `Analysts split on valuation`, sentiment_label: "neutral", published_at: new Date(baseDate - 11 * 86400000).toISOString(), source: "LiveMint", finbert_positive: 0.30 },
    ],
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// FORMATTING HELPERS
// ═══════════════════════════════════════════════════════════════════════════
const fmt = (v, d = 2) => v == null || isNaN(v) ? "—" : Number(v).toFixed(d);
const fmtPct = (v) => v == null || isNaN(v) ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;
const fmtPrice = (v) => v == null || isNaN(v) ? "—" : `₹${Number(v).toLocaleString("en-IN", { maximumFractionDigits: 0 })}`;
const fmtSign = (v, d = 2) => v == null || isNaN(v) ? "—" : `${v > 0 ? "+" : ""}${Number(v).toFixed(d)}`;

const sentimentColor = (label, t) =>
  label === "positive" ? t.accent : label === "negative" ? t.red : t.neutral;
const scoreColor = (v, t) =>
  v == null ? t.textMuted : v > 0.05 ? t.accent : v < -0.05 ? t.red : t.textSecondary;
const compositeLabel = (v) =>
  v == null ? "NO SIGNAL"
    : v > 0.5 ? "STRONG BULLISH" : v > 0.15 ? "LEAN BULLISH"
      : v < -0.5 ? "STRONG BEARISH" : v < -0.15 ? "LEAN BEARISH"
        : "NEUTRAL";

const vixTrendLabel = (v) =>
  v == null ? "—"
    : v < -0.1 ? "↓ falling"
      : v > 0.1 ? "↑ rising"
        : "→ stable";

const vixRegime = (v) =>
  v == null ? "—"
    : v < 15 ? "Calm"
      : v > 25 ? "Stressed"
        : "Normal";

// ═══════════════════════════════════════════════════════════════════════════
// PRIMITIVES
// ═══════════════════════════════════════════════════════════════════════════

// GlassCard — the foundational surface element. Every card uses this.
// Hover: lift 3px + soft teal glow. Smooth 0.28s cubic-bezier transition.
function GlassCard({ t, children, accent = false, warm = false, padding = 18,
  style = {}, hoverable = true, onClick = null,
  minHeight = null, className = "" }) {
  const [hovered, setHovered] = useState(false);
  const bg = accent ? t.surfaceAccent : warm ? t.surfaceWarm : t.surface;
  return (
    <div
      onClick={onClick}
      onMouseEnter={() => hoverable && setHovered(true)}
      onMouseLeave={() => hoverable && setHovered(false)}
      className={className}
      style={{
        position: "relative",
        background: bg,
        backdropFilter: "blur(14px) saturate(140%)",
        WebkitBackdropFilter: "blur(14px) saturate(140%)",
        border: `1px solid ${hovered ? t.borderStrong : t.border}`,
        borderRadius: 14,
        padding,
        cursor: onClick ? "pointer" : "default",
        transition: "transform 0.28s cubic-bezier(.2,.8,.25,1), box-shadow 0.28s cubic-bezier(.2,.8,.25,1), border-color 0.28s ease",
        transform: hovered && hoverable ? "translateY(-3px)" : "translateY(0)",
        boxShadow: hovered && hoverable ? t.glow : t.shadowResting,
        minHeight: minHeight || undefined,
        ...style,
      }}
    >
      {children}
    </div>
  );
}

// Tiny SVG sparkline. Takes an array of numbers (or {value} objects).
// Auto-fits, draws a smooth area + line, optional baseline.
function Sparkline({ data, t, color, width = 90, height = 28, fill = true,
  baseline = false, accessor = null }) {
  if (!data || data.length < 2) {
    return (
      <svg width={width} height={height} style={{ opacity: 0.3 }}>
        <line x1={0} y1={height / 2} x2={width} y2={height / 2}
          stroke={t.textDim} strokeWidth={1} strokeDasharray="2 2" />
      </svg>
    );
  }
  const get = accessor || ((d) => typeof d === "number" ? d : d.value ?? d.close ?? d.momentum ?? 0);
  const vals = data.map(get).filter((v) => typeof v === "number" && !isNaN(v));
  if (vals.length < 2) return null;
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = max - min || 1;
  const stepX = width / (vals.length - 1);
  const yFor = (v) => height - 2 - ((v - min) / range) * (height - 4);
  const points = vals.map((v, i) => `${i * stepX},${yFor(v)}`).join(" ");
  const stroke = color || t.teal;
  const areaPath =
    `M0,${height} L${vals.map((v, i) => `${i * stepX},${yFor(v)}`).join(" L")} L${width},${height} Z`;
  return (
    <svg width={width} height={height} style={{ overflow: "visible" }}>
      {baseline && (
        <line x1={0} y1={height / 2} x2={width} y2={height / 2}
          stroke={t.textDim} strokeWidth={0.5} strokeDasharray="2 2" opacity={0.5} />
      )}
      {fill && (
        <path d={areaPath} fill={stroke} opacity={0.16} />
      )}
      <polyline points={points} fill="none" stroke={stroke} strokeWidth={1.6}
        strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={(vals.length - 1) * stepX} cy={yFor(vals[vals.length - 1])}
        r={2.2} fill={stroke} />
    </svg>
  );
}

// Half-gauge (semicircle). Used for Dominant sentiment + Avg FRES cards.
// `value` ∈ [-1, 1] for sentiment, [0, 100] for FRES (autoscaled).
function HalfGauge({ value, t, color, range = [-1, 1], size = 78, label = null }) {
  const clamped = value == null ? null : Math.max(range[0], Math.min(range[1], value));
  const pct = clamped == null ? 0.5 : (clamped - range[0]) / (range[1] - range[0]);
  const angle = Math.PI * pct;        // 0 to π (left to right)
  const cx = size / 2;
  const cy = size * 0.78;
  const r = size * 0.42;
  const tickColor = t.textDim;
  const arcColor = color || t.teal;
  // Background arc
  const arcStart = `${cx - r},${cy}`;
  const arcEnd = `${cx + r},${cy}`;
  const bgPath = `M ${arcStart} A ${r} ${r} 0 0 1 ${arcEnd}`;
  // Filled arc up to value
  const valueX = cx - r * Math.cos(angle);
  const valueY = cy - r * Math.sin(angle);
  const fillPath = clamped == null
    ? null
    : `M ${arcStart} A ${r} ${r} 0 0 1 ${valueX},${valueY}`;
  // Needle
  const needleX = cx - r * 0.92 * Math.cos(angle);
  const needleY = cy - r * 0.92 * Math.sin(angle);
  return (
    <svg width={size} height={size * 0.85} style={{ display: "block" }}>
      <path d={bgPath} stroke={t.borderSoft} strokeWidth={6} fill="none" strokeLinecap="round" />
      {fillPath && (
        <path d={fillPath} stroke={arcColor} strokeWidth={6} fill="none" strokeLinecap="round" />
      )}
      {/* Tick marks at quartiles */}
      {[0, 0.25, 0.5, 0.75, 1].map((p) => {
        const a = Math.PI * p;
        const x1 = cx - r * Math.cos(a);
        const y1 = cy - r * Math.sin(a);
        const x2 = cx - (r + 4) * Math.cos(a);
        const y2 = cy - (r + 4) * Math.sin(a);
        return <line key={p} x1={x1} y1={y1} x2={x2} y2={y2} stroke={tickColor} strokeWidth={0.8} />;
      })}
      {clamped != null && (
        <>
          <line x1={cx} y1={cy} x2={needleX} y2={needleY}
            stroke={t.textPrimary} strokeWidth={1.6} strokeLinecap="round" />
          <circle cx={cx} cy={cy} r={3.5} fill={t.textPrimary} />
        </>
      )}
      {label && (
        <text x={cx} y={cy + 12} fontSize={9} fill={t.textMuted}
          fontFamily="'JetBrains Mono', monospace" textAnchor="middle">
          {label}
        </text>
      )}
    </svg>
  );
}

// Composite score gauge — the big centerpiece. Semicircular arc with
// gradient bands (red → amber → green) and a black needle. Shows the
// numeric value + "LEAN BEARISH"-style label below.
function CompositeGauge({ score, t, large = true }) {
  const size = large ? 280 : 200;
  const v = score == null ? 0 : Math.max(-1, Math.min(1, score));
  const hasValue = score != null;
  const pct = (v + 1) / 2;
  const angle = Math.PI * pct;
  const cx = size / 2;
  const cy = size * 0.66;
  const r = size * 0.36;
  const startX = cx - r;
  const endX = cx + r;
  // Gradient track (red→amber→green)
  const gradientId = `comp-grad-${large ? "lg" : "sm"}`;
  const trackBg = `${t.borderSoft}`;
  const needleX = cx - r * Math.cos(angle);
  const needleY = cy - r * Math.sin(angle);
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8 }}>
      <svg width={size} height={size * 0.78} style={{ display: "block", overflow: "visible" }}>
        <defs>
          <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor={t.red} />
            <stop offset="50%" stopColor={t.gold} />
            <stop offset="100%" stopColor={t.accent} />
          </linearGradient>
        </defs>
        {/* Track shadow */}
        <path d={`M ${startX},${cy} A ${r} ${r} 0 0 1 ${endX},${cy}`}
          stroke={trackBg} strokeWidth={18} fill="none" strokeLinecap="round" />
        {/* Gradient arc */}
        <path d={`M ${startX},${cy} A ${r} ${r} 0 0 1 ${endX},${cy}`}
          stroke={`url(#${gradientId})`} strokeWidth={14} fill="none"
          strokeLinecap="round" opacity={hasValue ? 0.92 : 0.4} />
        {/* Tick marks */}
        {[0, 0.25, 0.5, 0.75, 1].map((p) => {
          const a = Math.PI * p;
          const x1 = cx - (r + 6) * Math.cos(a);
          const y1 = cy - (r + 6) * Math.sin(a);
          const x2 = cx - (r + 13) * Math.cos(a);
          const y2 = cy - (r + 13) * Math.sin(a);
          return <line key={p} x1={x1} y1={y1} x2={x2} y2={y2}
            stroke={t.textMuted} strokeWidth={1.2} />;
        })}
        {/* Needle */}
        {hasValue && (
          <>
            <line x1={cx} y1={cy} x2={needleX} y2={needleY}
              stroke={t.textPrimary} strokeWidth={2.4} strokeLinecap="round" />
            <circle cx={cx} cy={cy} r={8} fill={t.surfaceSolid}
              stroke={t.textPrimary} strokeWidth={2} />
          </>
        )}
        {/* Value label inside arc */}
        <text x={cx} y={cy - 18} fontSize={large ? 42 : 30} fontWeight={700}
          fill={hasValue ? scoreColor(v, t) : t.textMuted}
          fontFamily="'Instrument Serif', 'IBM Plex Sans', serif"
          textAnchor="middle" letterSpacing={-1}>
          {hasValue ? fmtSign(v, 2) : "—"}
        </text>
        <text x={cx} y={cy + 4} fontSize={11} fill={t.textMuted}
          fontFamily="'JetBrains Mono', monospace" textAnchor="middle"
          letterSpacing={1.4} fontWeight={600}>
          {compositeLabel(v)}
        </text>
      </svg>
      {/* Bearish / Neutral / Bullish labels */}
      <div style={{
        display: "flex", justifyContent: "space-between", width: size - 20,
        fontSize: 10, color: t.textMuted, fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: 0.6, marginTop: -8,
      }}>
        <span style={{ color: t.red }}>Bearish −1</span>
        <span>Neutral 0</span>
        <span style={{ color: t.accent }}>Bullish +1</span>
      </div>
    </div>
  );
}

// LLM comparison radar (mini pentagon). Compares Groq vs Llama on 5 axes
// derived from their scores + the underlying article distribution.
function LLMRadar({ groq, llama, sentimentDist, t, size = 160 }) {
  const cx = size / 2;
  const cy = size / 2 + 8;
  const r = size * 0.36;
  const axes = ["Direction", "Confidence", "Positive", "Negative", "Neutral"];
  // Build vertex coords for each axis on a unit-radius pentagon
  const angleFor = (i) => -Math.PI / 2 + (2 * Math.PI * i) / axes.length;
  const axisPts = axes.map((_, i) => ({
    x: cx + r * Math.cos(angleFor(i)),
    y: cy + r * Math.sin(angleFor(i)),
  }));
  // Normalize each LLM into the 5 axes (0..1)
  const series = (score) => {
    const s = score ?? 0;
    return [
      (s + 1) / 2,                                 // Direction
      Math.min(1, Math.abs(s) * 1.2),              // Confidence
      sentimentDist?.pos ?? 0,                     // Positive
      sentimentDist?.neg ?? 0,                     // Negative
      sentimentDist?.neu ?? 0,                     // Neutral
    ];
  };
  const pointsFor = (vec) =>
    vec.map((v, i) => {
      const x = cx + r * v * Math.cos(angleFor(i));
      const y = cy + r * v * Math.sin(angleFor(i));
      return `${x},${y}`;
    }).join(" ");
  const groqVec = series(groq);
  const llamaVec = series(llama);
  return (
    <svg width={size} height={size + 20} style={{ display: "block" }}>
      {/* Concentric guides */}
      {[0.33, 0.66, 1].map((k) => (
        <polygon key={k}
          points={axes.map((_, i) => {
            const x = cx + r * k * Math.cos(angleFor(i));
            const y = cy + r * k * Math.sin(angleFor(i));
            return `${x},${y}`;
          }).join(" ")}
          fill="none" stroke={t.borderSoft} strokeWidth={0.8} />
      ))}
      {/* Spokes */}
      {axisPts.map((p, i) => (
        <line key={i} x1={cx} y1={cy} x2={p.x} y2={p.y}
          stroke={t.borderSoft} strokeWidth={0.8} />
      ))}
      {/* Llama (filled, blue) */}
      <polygon points={pointsFor(llamaVec)}
        fill={t.blue} fillOpacity={0.18}
        stroke={t.blue} strokeWidth={1.2} />
      {/* Groq (filled, gold) */}
      <polygon points={pointsFor(groqVec)}
        fill={t.gold} fillOpacity={0.22}
        stroke={t.gold} strokeWidth={1.2} />
      {/* Axis labels */}
      {axes.map((label, i) => {
        const x = cx + (r + 10) * Math.cos(angleFor(i));
        const y = cy + (r + 10) * Math.sin(angleFor(i));
        return (
          <text key={label} x={x} y={y} fontSize={8.5} fill={t.textMuted}
            fontFamily="'JetBrains Mono', monospace"
            textAnchor="middle" dominantBaseline="middle"
            letterSpacing={0.3}>
            {label}
          </text>
        );
      })}
    </svg>
  );
}

// Stacked-timeline distribution. Renders N vertical "tiles" representing
// the FinBERT positive/negative/neutral split over time. Inspired by
// GitHub's contribution graph but with three-color stacks per cell.
function StackedTimeline({ articles, avgPos, avgNeg, avgNeu, t }) {
  // Bucket articles into ~14 weekly slots; if too few, just spread evenly
  const buckets = useMemo(() => {
    const N = 14;
    if (!articles || articles.length === 0) {
      // Empty placeholder buckets
      return Array.from({ length: N }, () => ({ pos: 0, neg: 0, neu: 0, count: 0 }));
    }
    // Sort by date and bin
    const sorted = [...articles].sort((a, b) =>
      new Date(a.published_at) - new Date(b.published_at));
    const result = Array.from({ length: N }, () => ({ pos: 0, neg: 0, neu: 0, count: 0 }));
    sorted.forEach((a, idx) => {
      const slot = Math.min(N - 1, Math.floor((idx / sorted.length) * N));
      result[slot].pos += a.finbert_positive ?? (a.sentiment_label === "positive" ? 1 : 0);
      result[slot].neg += a.finbert_negative ?? (a.sentiment_label === "negative" ? 1 : 0);
      result[slot].neu += a.finbert_neutral ?? (a.sentiment_label === "neutral" ? 1 : 0);
      result[slot].count += 1;
    });
    return result.map(b => {
      if (b.count === 0) return b;
      const sum = b.pos + b.neg + b.neu || 1;
      return { ...b, pos: b.pos / sum, neg: b.neg / sum, neu: b.neu / sum };
    });
  }, [articles]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {[
        { key: "pos", label: "Positive", color: t.accent, value: avgPos },
        { key: "neg", label: "Negative", color: t.red, value: avgNeg },
        { key: "neu", label: "Neutral", color: t.neutral, value: avgNeu },
      ].map((row) => (
        <div key={row.key} style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{
            width: 60, fontSize: 11, color: t.textSecondary,
            fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.3,
          }}>{row.label}</span>
          <div style={{ display: "flex", gap: 3, flex: 1, height: 22 }}>
            {buckets.map((b, i) => {
              const intensity = b.count === 0 ? 0 : b[row.key];
              return (
                <div key={i} style={{
                  flex: 1,
                  background: intensity > 0
                    ? row.color
                    : t.borderSoft,
                  opacity: intensity > 0 ? 0.25 + intensity * 0.75 : 1,
                  borderRadius: 3,
                  transition: "all 0.3s ease",
                }} />
              );
            })}
          </div>
          <span style={{
            width: 50, textAlign: "right", fontSize: 11, color: row.color,
            fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
          }}>
            {row.value != null ? `${(row.value * 100).toFixed(0)}%` : "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

// Card label (small uppercase mono header at the top of every card)
function CardLabel({ children, t, color = null, icon = null, right = null }) {
  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "space-between",
      marginBottom: 8, gap: 8,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        {icon}
        <span style={{
          fontSize: 10, fontWeight: 700, color: color || t.textMuted,
          textTransform: "uppercase", letterSpacing: 1.5,
          fontFamily: "'JetBrains Mono', monospace",
        }}>{children}</span>
      </div>
      {right}
    </div>
  );
}
// ═══════════════════════════════════════════════════════════════════════════
// METRIC CARDS — each card is small, glass, and includes a sparkline or gauge
// ═══════════════════════════════════════════════════════════════════════════

// Articles card with mini timeline of article counts (or just a static line)
function ArticlesCard({ count, articles, t }) {
  // Build a tiny series — count of articles per slot
  const series = useMemo(() => {
    if (!articles || articles.length === 0) return null;
    const buckets = Array(8).fill(0);
    const sorted = [...articles].sort((a, b) =>
      new Date(a.published_at) - new Date(b.published_at));
    sorted.forEach((_, idx) => {
      buckets[Math.min(7, Math.floor((idx / sorted.length) * 8))]++;
    });
    return buckets;
  }, [articles]);
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t} icon={<Newspaper size={11} color={t.textMuted} />}>
        Articles
      </CardLabel>
      <div style={{
        fontSize: 36, fontWeight: 700, color: t.textPrimary,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, marginBottom: 4,
        letterSpacing: -1,
      }}>
        {count ?? 0}
      </div>
      <div style={{ fontSize: 11, color: t.textMuted, marginBottom: 10 }}>
        pre-listing
      </div>
      {series ? (
        <Sparkline data={series} t={t} color={t.teal} width={120} height={26} />
      ) : (
        <div style={{ display: "flex", gap: 2, height: 26, alignItems: "flex-end" }}>
          {Array.from({ length: 18 }).map((_, i) => (
            <div key={i} style={{
              flex: 1, height: 2 + (i % 5) * 1.2, background: t.borderSoft, borderRadius: 1,
            }} />
          ))}
        </div>
      )}
    </GlassCard>
  );
}

// Dominant sentiment card with half-gauge
function DominantCard({ dominant, avgPos, avgNeg, t }) {
  const value = dominant === "positive" ? (avgPos ?? 0.5)
    : dominant === "negative" ? -(avgNeg ?? 0.5)
      : 0;
  const color = dominant === "positive" ? t.accent
    : dominant === "negative" ? t.red
      : t.neutral;
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t} icon={<Activity size={11} color={t.textMuted} />}>
        Dominant
      </CardLabel>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 8 }}>
        <div>
          <div style={{
            fontSize: 26, fontWeight: 700, color,
            fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1,
            marginBottom: 4, textTransform: "capitalize",
          }}>
            {dominant ?? "—"}
          </div>
          <div style={{ fontSize: 11, color: t.textMuted }}>FinBERT</div>
        </div>
        <HalfGauge value={value} t={t} color={color} range={[-1, 1]} size={86} />
      </div>
    </GlassCard>
  );
}

// LLM Comparative Scores — Groq vs Llama (radar)
function LLMComparativeCard({ groq, llama, sentimentDist, t }) {
  const avg = ((groq ?? 0) + (llama ?? 0)) / 2;
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t} icon={<Sparkles size={11} color={t.textMuted} />}
        right={<span style={{
          fontSize: 11, color: scoreColor(avg, t),
          fontFamily: "'JetBrains Mono', monospace",
          fontWeight: 600
        }}>
          ({fmtSign(avg)})
        </span>}>
        LLM Comparative Scores
      </CardLabel>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <div style={{
              fontSize: 18, fontWeight: 700, color: t.gold,
              fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1
            }}>
              GROQ
            </div>
            <div style={{
              fontSize: 9, color: t.textMuted,
              fontFamily: "'JetBrains Mono', monospace", marginTop: 2
            }}>
              LLaMA-3.3-70B
            </div>
            <div style={{
              fontSize: 14, fontWeight: 700,
              color: scoreColor(groq, t), marginTop: 2,
              fontFamily: "'JetBrains Mono', monospace"
            }}>
              {fmtSign(groq)}
            </div>
          </div>
        </div>
        <LLMRadar groq={groq} llama={llama} sentimentDist={sentimentDist} t={t} size={120} />
        <div style={{ display: "flex", flexDirection: "column", gap: 14, textAlign: "right" }}>
          <div>
            <div style={{
              fontSize: 18, fontWeight: 700, color: t.blue,
              fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1
            }}>
              LLaMA
            </div>
            <div style={{
              fontSize: 9, color: t.textMuted,
              fontFamily: "'JetBrains Mono', monospace", marginTop: 2
            }}>
              LLaMA-3.1-8B
            </div>
            <div style={{
              fontSize: 14, fontWeight: 700,
              color: scoreColor(llama, t), marginTop: 2,
              fontFamily: "'JetBrains Mono', monospace"
            }}>
              {fmtSign(llama)}
            </div>
          </div>
        </div>
      </div>
    </GlassCard>
  );
}

// Avg FRES gauge card
function FRESCard({ value, t }) {
  const v = value;
  const color = v == null ? t.textMuted
    : v > 60 ? t.accent
      : v < 40 ? t.red
        : t.gold;
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t}>Avg FRES</CardLabel>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <HalfGauge value={v ?? 0} t={t} color={color} range={[0, 100]} size={68} />
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{
            fontSize: 28, fontWeight: 700, color,
            fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, letterSpacing: -1,
          }}>
            {v != null ? fmt(v, 1) : "—"}
          </div>
          <div style={{
            fontSize: 10, color: t.textMuted, marginTop: 2,
            fontFamily: "'JetBrains Mono', monospace"
          }}>
            {v == null ? "no corpus"
              : v > 60 ? "easy read"
                : v < 40 ? "complex"
                  : "moderate"}
          </div>
        </div>
      </div>
    </GlassCard>
  );
}

// Market Mood card with sparkline (fed by nifty_price_series mood proxy)
function MarketMoodCard({ score, niftySeries, t }) {
  const color = scoreColor(score, t);
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t}>Market Mood</CardLabel>
      <div style={{
        fontSize: 30, fontWeight: 700, color,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, letterSpacing: -1,
        marginBottom: 4,
      }}>
        {fmtSign(score)}
      </div>
      <div style={{ fontSize: 11, color: t.textMuted, marginBottom: 8 }}>
        Nifty + VIX
      </div>
      <Sparkline data={niftySeries} t={t} color={color}
        width={140} height={28} accessor={(d) => d.close} />
    </GlassCard>
  );
}

// Macro Score card with sparkline
function MacroScoreCard({ score, available, t }) {
  const color = available ? scoreColor(score, t) : t.textMuted;
  return (
    <GlassCard t={t} minHeight={130}>
      <CardLabel t={t}>Macro Score</CardLabel>
      <div style={{
        fontSize: 30, fontWeight: 700, color,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, letterSpacing: -1,
        marginBottom: 4,
      }}>
        {available ? fmtSign(score) : "—"}
      </div>
      <div style={{ fontSize: 11, color: t.textMuted, marginBottom: 8 }}>
        {available ? "FRED composite" : "FRED key not set"}
      </div>
      {available && (
        <div style={{ display: "flex", gap: 1.5, height: 24, alignItems: "flex-end" }}>
          {Array.from({ length: 24 }).map((_, i) => {
            const h = 6 + Math.abs(Math.sin(i / 2 + (score ?? 0) * 3)) * 16;
            return (
              <div key={i} style={{
                flex: 1, height: h, borderRadius: 1.5,
                background: i > 16 ? color : t.borderSoft,
                opacity: i > 16 ? 0.7 : 1,
              }} />
            );
          })}
        </div>
      )}
    </GlassCard>
  );
}

// Nifty / VIX cards (compact, with sparkline)
function NiftyCard({ price, return5d, series, t }) {
  const ret = return5d;
  const color = ret == null ? t.textMuted : ret > 0 ? t.accent : t.red;
  return (
    <GlassCard t={t} minHeight={110}>
      <CardLabel t={t}>Nifty T-1</CardLabel>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <div>
          <div style={{
            fontSize: 26, fontWeight: 700, color: t.textPrimary,
            fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, letterSpacing: -1,
          }}>
            {fmtPrice(price)}
          </div>
          <div style={{
            fontSize: 11, color, marginTop: 4,
            fontFamily: "'JetBrains Mono', monospace", fontWeight: 600
          }}>
            {fmtPct(ret)} <span style={{ color: t.textMuted, fontWeight: 400 }}>(5d)</span>
          </div>
        </div>
        <Sparkline data={series} t={t} color={color}
          width={84} height={32} accessor={(d) => d.close} />
      </div>
    </GlassCard>
  );
}

function VIXCard({ vix, trend, t }) {
  const trendLabel = trend == null ? "—"
    : trend < -0.1 ? "↓ falling"
      : trend > 0.1 ? "↑ rising"
        : "→ stable";
  const regimeColor = vix == null ? t.textMuted
    : vix < 15 ? t.accent
      : vix > 25 ? t.red
        : t.gold;
  // Generate a tiny stylized sparkline based on trend direction
  const series = useMemo(() => {
    const slope = trend ?? 0;
    return Array.from({ length: 12 }, (_, i) => ({
      value: (vix ?? 15) + Math.sin(i / 1.5) * 1.5 + slope * (i - 6) * 0.6,
    }));
  }, [vix, trend]);
  return (
    <GlassCard t={t} minHeight={110}>
      <CardLabel t={t}>India VIX</CardLabel>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <div>
          <div style={{
            fontSize: 26, fontWeight: 700, color: t.textPrimary,
            fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1, letterSpacing: -1,
          }}>
            {fmt(vix, 1)}
          </div>
          <div style={{
            fontSize: 11, color: regimeColor, marginTop: 4,
            fontFamily: "'JetBrains Mono', monospace", fontWeight: 600
          }}>
            {trendLabel}
          </div>
        </div>
        <Sparkline data={series} t={t} color={regimeColor}
          width={84} height={32} accessor={(d) => d.value} />
      </div>
    </GlassCard>
  );
}

// Wide trend chart (for the right-side panel)
function TrendChart({ series, t, height = 110, color = null }) {
  if (!series || series.length === 0) {
    return (
      <div style={{
        height, display: "flex", alignItems: "center", justifyContent: "center",
        color: t.textMuted, fontSize: 13, fontFamily: "'JetBrains Mono', monospace",
        opacity: 0.6,
      }}>
        no series data
      </div>
    );
  }
  const c = color || t.teal;
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={series} margin={{ top: 6, right: 6, left: 6, bottom: 0 }}>
        <defs>
          <linearGradient id="trendGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={c} stopOpacity={0.35} />
            <stop offset="100%" stopColor={c} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <Area type="monotone" dataKey="close" stroke={c} strokeWidth={1.8}
          fill="url(#trendGrad)" />
      </AreaChart>
    </ResponsiveContainer>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// LLM PANEL — Groq + Llama summaries with badges, source attribution
// ═══════════════════════════════════════════════════════════════════════════
function LLMPanel({ groqScore, groqSummary, llamaScore, llamaSummary, newsSource, t }) {
  const isKnowledgeOnly = newsSource === "unavailable" || newsSource === "knowledge_only" ||
    (groqSummary || "").toLowerCase().includes("market+macro") ||
    (groqSummary || "").toLowerCase().includes("knowledge-only");
  return (
    <GlassCard t={t} hoverable={false} padding={20}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
        <CardLabel t={t} icon={<Sparkles size={12} color={t.textMuted} />}>
          LLM Analysis
        </CardLabel>
        {isKnowledgeOnly && (
          <span style={{
            fontSize: 9, padding: "3px 8px", borderRadius: 12,
            background: t.goldSoft, color: t.gold,
            fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.6,
            fontWeight: 700, textTransform: "uppercase",
          }}>
            Market-only mode
          </span>
        )}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        {/* Groq */}
        <div style={{
          padding: "12px 14px", borderRadius: 10,
          background: t.goldSoft, border: `1px solid ${t.borderSoft}`,
        }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <span style={{
              fontSize: 12, fontWeight: 700, color: t.gold,
              fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", letterSpacing: 0.5
            }}>
              GROQ · LLaMA-3.3-70B
            </span>
            <span style={{
              fontSize: 13, fontWeight: 700, color: scoreColor(groqScore, t),
              fontFamily: "'JetBrains Mono', monospace"
            }}>
              {fmtSign(groqScore)}
            </span>
          </div>
          <p style={{ margin: 0, fontSize: 12.5, color: t.textSecondary, lineHeight: 1.55 }}>
            {groqSummary || "No Groq summary available."}
          </p>
        </div>
        {/* Llama */}
        <div style={{
          padding: "12px 14px", borderRadius: 10,
          background: `${t.blue}1a`, border: `1px solid ${t.borderSoft}`,
        }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <span style={{
              fontSize: 12, fontWeight: 700, color: t.blue,
              fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", letterSpacing: 0.5
            }}>
              LLaMA-3.1-8B · Cross-check
            </span>
            <span style={{
              fontSize: 13, fontWeight: 700, color: scoreColor(llamaScore, t),
              fontFamily: "'JetBrains Mono', monospace"
            }}>
              {fmtSign(llamaScore)}
            </span>
          </div>
          <p style={{ margin: 0, fontSize: 12.5, color: t.textSecondary, lineHeight: 1.55 }}>
            {llamaSummary || "No Llama summary available."}
          </p>
        </div>
      </div>
      <div style={{
        marginTop: 12, fontSize: 10.5, color: t.textMuted,
        fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.4
      }}>
        Composite uses average: {fmtSign(((groqScore || 0) + (llamaScore || 0)) / 2)}
      </div>
    </GlassCard>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// EMPTY / ERROR STATES — beautiful textured panels instead of plain text
// ═══════════════════════════════════════════════════════════════════════════
function NotFoundScreen({ ipoName, listingDate, hint, onReset, onForceAnalyze, t }) {
  return (
    <GlassCard t={t} hoverable={false} padding={48}
      style={{ textAlign: "center", marginTop: 24 }}>
      <div style={{
        width: 64, height: 64, borderRadius: "50%",
        background: t.goldSoft, color: t.gold,
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        marginBottom: 16,
      }}>
        <SearchX size={28} />
      </div>
      <h2 style={{
        margin: "0 0 8px", fontSize: 22, fontWeight: 700, color: t.textPrimary,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", letterSpacing: -0.5
      }}>
        IPO not found
      </h2>
      <p style={{ margin: "0 0 6px", fontSize: 14, color: t.textSecondary }}>
        Couldn't find <strong style={{ color: t.textPrimary }}>{ipoName}</strong>
        {listingDate ? ` (${listingDate})` : ""} in the master index.
      </p>
      {hint && (
        <p style={{
          margin: "0 0 24px", fontSize: 12, color: t.textMuted,
          fontFamily: "'JetBrains Mono', monospace"
        }}>
          {hint}
        </p>
      )}
      <div style={{ display: "flex", gap: 10, justifyContent: "center", marginTop: 16 }}>
        <button onClick={onReset} style={{
          background: "transparent", border: `1px solid ${t.border}`,
          color: t.textSecondary, padding: "10px 20px", borderRadius: 8,
          cursor: "pointer", fontSize: 13, fontFamily: "inherit", fontWeight: 500,
        }}>Search again</button>
        {onForceAnalyze && (
          <button onClick={onForceAnalyze} style={{
            background: t.gold, border: "none", color: "#fff",
            padding: "10px 22px", borderRadius: 8, cursor: "pointer",
            fontSize: 13, fontFamily: "'JetBrains Mono', monospace",
            fontWeight: 700, letterSpacing: 0.5,
          }}>⚡ Analyze Anyway</button>
        )}
      </div>
    </GlassCard>
  );
}

function ErrorScreen({ message, onRetry, t }) {
  return (
    <GlassCard t={t} hoverable={false} padding={48}
      style={{ textAlign: "center", marginTop: 24 }}>
      <div style={{
        width: 64, height: 64, borderRadius: "50%",
        background: t.redSoft, color: t.red,
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        marginBottom: 16,
      }}>
        <ServerCrash size={28} />
      </div>
      <h2 style={{
        margin: "0 0 8px", fontSize: 22, fontWeight: 700, color: t.textPrimary,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif"
      }}>
        Pipeline error
      </h2>
      <p style={{
        margin: "0 0 24px", fontSize: 13, color: t.textSecondary,
        maxWidth: 480, marginLeft: "auto", marginRight: "auto", lineHeight: 1.6
      }}>
        {message}
      </p>
      <button onClick={onRetry} style={{
        background: t.accent, border: "none", color: "#fff",
        padding: "10px 22px", borderRadius: 8, cursor: "pointer",
        fontSize: 13, fontFamily: "'JetBrains Mono', monospace",
        fontWeight: 700, letterSpacing: 0.5,
      }}>Retry</button>
    </GlassCard>
  );
}
// ═══════════════════════════════════════════════════════════════════════════
// DASHBOARD GRID — the main 3-row layout matching the target design
// ═══════════════════════════════════════════════════════════════════════════
function DashboardGrid({ d, t, sentimentDist, weightsVisible, setWeightsVisible }) {
  const isKnowledgeOnly = d.news_source === "unavailable" || d.news_source === "knowledge_only";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* ═══ ROW 1 — six metric cards ═══════════════════════════════════ */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1.05fr 1.05fr 2fr 0.9fr 1.05fr 1.05fr",
        gap: 14,
      }}>
        <ArticlesCard count={d.article_count} articles={d.articles} t={t} />
        <DominantCard dominant={d.dominant_sentiment}
          avgPos={d.avg_positive} avgNeg={d.avg_negative} t={t} />
        <LLMComparativeCard groq={d.groq_score} llama={d.llama_score}
          sentimentDist={sentimentDist} t={t} />
        <FRESCard value={d.avg_flesch_score} t={t} />
        <MarketMoodCard score={d.market_mood_score}
          niftySeries={d.nifty_price_series} t={t} />
        <MacroScoreCard score={d.macro_score}
          available={d.macro_available} t={t} />
      </div>

      {/* ═══ ROW 2 — Nifty + VIX + wide trend chart ═════════════════════ */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1.4fr 1fr 3fr",
        gap: 14,
      }}>
        <NiftyCard price={d.nifty_price_t1} return5d={d.nifty_return_5d}
          series={d.nifty_price_series} t={t} />
        <VIXCard vix={d.vix_t1} trend={d.vix_trend} t={t} />
        <GlassCard t={t} padding={16}>
          <CardLabel t={t} icon={<TrendingUp size={11} color={t.textMuted} />}>
            Nifty 50 — pre-listing window
          </CardLabel>
          <TrendChart series={d.nifty_price_series} t={t}
            color={d.nifty_return_5d > 0 ? t.accent : t.red} height={68} />
        </GlassCard>
      </div>

      {/* ═══ ROW 3 — FinBERT distribution + Composite gauge ═════════════ */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1.7fr 1fr",
        gap: 16,
      }}>
        {/* FinBERT panel — distribution + LLM analysis stacked */}
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <GlassCard t={t} padding={20}>
            <CardLabel t={t} icon={<Activity size={11} color={t.textMuted} />}>
              FinBERT Sentiment Distribution
            </CardLabel>
            <div style={{ marginTop: 10 }}>
              <StackedTimeline articles={d.articles}
                avgPos={d.avg_positive} avgNeg={d.avg_negative}
                avgNeu={d.avg_neutral} t={t} />
            </div>
            {/* Mini LLM scores at bottom */}
            <div style={{
              display: "flex", justifyContent: "space-between", marginTop: 18,
              paddingTop: 14, borderTop: `1px solid ${t.borderSoft}`,
            }}>
              <ScoreChip t={t} label="GROQ · LLaMA-3.3-70B"
                score={d.groq_score} color={t.gold} />
              <ScoreChip t={t} label="LLaMA-3.1-8B"
                score={d.llama_score} color={t.blue} align="right" />
            </div>
          </GlassCard>

          {/* LLM Analysis OR data-state panel */}
          {isKnowledgeOnly ? (
            <KnowledgeOnlyPanel d={d} t={t} />
          ) : (
            <LLMPanel groqScore={d.groq_score} groqSummary={d.groq_summary}
              llamaScore={d.llama_score} llamaSummary={d.llama_summary}
              newsSource={d.news_source} t={t} />
          )}
        </div>

        {/* Composite Score panel */}
        <GlassCard t={t} accent padding={22} hoverable={false}>
          <CardLabel t={t} icon={<CircleDot size={11} color={t.accent} />}
            color={t.textSecondary}>
            Composite Score
          </CardLabel>
          <div style={{ display: "flex", justifyContent: "center", marginTop: 10 }}>
            <CompositeGauge score={d.composite_score} t={t} large />
          </div>
          <div style={{ marginTop: 22 }}>
            <div style={{
              fontSize: 10.5, color: t.textMuted, marginBottom: 10,
              fontFamily: "'JetBrains Mono', monospace", letterSpacing: 1.2,
              textTransform: "uppercase", fontWeight: 600,
            }}>
              Score weights
            </div>
            <div style={{
              display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8,
            }}>
              <WeightRow t={t} label="Groq+Llama" pct={28}
                visible={weightsVisible.groqLlama}
                onToggle={() => setWeightsVisible(w => ({ ...w, groqLlama: !w.groqLlama }))} />
              <WeightRow t={t} label="FinBERT" pct={22}
                visible={weightsVisible.finbert}
                onToggle={() => setWeightsVisible(w => ({ ...w, finbert: !w.finbert }))} />
              <WeightRow t={t} label="Macro (FRED)" pct={8}
                visible={weightsVisible.macro}
                onToggle={() => setWeightsVisible(w => ({ ...w, macro: !w.macro }))} />
              <WeightRow t={t} label="Momentum" pct={10}
                visible={weightsVisible.momentum}
                onToggle={() => setWeightsVisible(w => ({ ...w, momentum: !w.momentum }))} />
              <WeightRow t={t} label="Readability" pct={5}
                visible={weightsVisible.readability}
                onToggle={() => setWeightsVisible(w => ({ ...w, readability: !w.readability }))} />
              <WeightRow t={t} label="Market Mood" pct={27}
                visible={true} disabled />
            </div>
          </div>
        </GlassCard>
      </div>

      {/* ═══ ROW 4 — Macro Context (FRED) ═══════════════════════════════ */}
      <MacroFREDPanel d={d} t={t} />

      {/* ═══ ROW 5 — VIX + Nifty stat tables ════════════════════════════ */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: 16,
      }}>
        <DataTable t={t} title="India VIX (pre-listing)" rows={[
          { label: "VIX on T-1", value: fmt(d.vix_t1, 1) },
          { label: "Window average", value: fmt(d.vix_avg_window, 1) },
          { label: "VIX trend", value: vixTrendLabel(d.vix_trend) },
          {
            label: "Regime", value: vixRegime(d.vix_t1),
            color: d.vix_t1 == null ? t.textMuted
              : d.vix_t1 < 15 ? t.accent
                : d.vix_t1 > 25 ? t.red : t.textPrimary
          },
        ]} />
        <DataTable t={t} title="Nifty 50 snapshot" rows={[
          { label: "Close T-1", value: fmtPrice(d.nifty_price_t1) },
          {
            label: "5-day return", value: fmtPct(d.nifty_return_5d),
            color: d.nifty_return_5d == null ? t.textMuted
              : d.nifty_return_5d > 0 ? t.accent : t.red
          },
          { label: "MACD (12,26)", value: fmtSign(d.nifty_macd, 0) },
          {
            label: "Above SMA-20",
            value: d.nifty_above_sma == null ? "—"
              : (d.nifty_above_sma ? "Yes" : "No"),
            color: d.nifty_above_sma == null ? t.textMuted
              : d.nifty_above_sma ? t.accent : t.red
          },
        ]} />
      </div>

      {/* ═══ ROW 6 — Big charts: Nifty 30-day price + Sentiment momentum ══ */}
      {Array.isArray(d.nifty_price_series) && d.nifty_price_series.length > 0 && (
        <BigAreaChart
          t={t}
          title="Nifty 50 — 30-day pre-listing price"
          data={d.nifty_price_series}
          dataKey="close"
          color={d.nifty_return_5d > 0 ? t.accent : t.red}
          yFormat={(v) => `₹${(v / 1000).toFixed(1)}k`}
          tooltipPrefix="₹"
          height={240}
        />
      )}
      {Array.isArray(d.sentiment_momentum_series) && d.sentiment_momentum_series.length > 0 && (
        <BigAreaChart
          t={t}
          title="Sentiment momentum — FinBERT positive score, day by day"
          data={d.sentiment_momentum_series}
          dataKey="positive_score"
          color={t.blue}
          yFormat={(v) => `${Math.round(v * 100)}%`}
          tooltipFormat={(v) => `${Math.round(v * 100)}%`}
          yDomain={[0, 1]}
          height={220}
        />
      )}

      {/* ═══ ROW 7 — Articles list (if present) ═════════════════════════ */}
      {d.articles && d.articles.length > 0 && (
        <GlassCard t={t} padding={20}>
          <CardLabel t={t} icon={<Newspaper size={11} color={t.textMuted} />}>
            Pre-listing articles ({d.articles.length})
          </CardLabel>
          <div style={{
            display: "grid", gap: 10, marginTop: 8,
            maxHeight: 320, overflowY: "auto",
          }}>
            {d.articles.slice(0, 12).map((a, i) => (
              <ArticleRow key={i} article={a} t={t} />
            ))}
          </div>
        </GlassCard>
      )}
    </div>
  );
}

function ScoreChip({ t, label, score, color, align = "left" }) {
  return (
    <div style={{ textAlign: align }}>
      <div style={{
        fontSize: 10.5, color: t.textMuted,
        fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.4,
        marginBottom: 3,
      }}>
        {label}
      </div>
      <div style={{
        fontSize: 18, fontWeight: 700, color: scoreColor(score, t),
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", letterSpacing: -0.4,
      }}>
        {fmtSign(score)}
      </div>
    </div>
  );
}

function WeightRow({ t, label, pct, visible, onToggle, disabled = false }) {
  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "space-between",
      padding: "7px 12px",
      background: t.surfaceHover,
      border: `1px solid ${t.borderSoft}`, borderRadius: 8,
      opacity: visible ? 1 : 0.4,
      transition: "opacity 0.2s ease",
    }}>
      <span style={{ fontSize: 11, color: t.textSecondary, fontWeight: 500 }}>
        {label} <span style={{
          color: t.textMuted, fontFamily: "'JetBrains Mono', monospace",
        }}>{pct}%</span>
      </span>
      {!disabled && (
        <button onClick={onToggle} style={{
          background: "transparent", border: "none", color: t.textMuted,
          cursor: "pointer", padding: 0, display: "flex", alignItems: "center",
        }}>
          {visible ? <Eye size={12} /> : <EyeOff size={12} />}
        </button>
      )}
    </div>
  );
}

function ArticleRow({ article, t }) {
  const c = sentimentColor(article.sentiment_label, t);
  return (
    <div style={{
      display: "flex", alignItems: "flex-start", gap: 10,
      padding: "10px 12px", borderRadius: 8,
      background: t.surfaceHover, border: `1px solid ${t.borderSoft}`,
      transition: "transform 0.2s ease, border-color 0.2s ease",
    }}
      onMouseEnter={(e) => {
        e.currentTarget.style.transform = "translateX(2px)";
        e.currentTarget.style.borderColor = t.borderStrong;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.transform = "translateX(0)";
        e.currentTarget.style.borderColor = t.borderSoft;
      }}>
      <div style={{
        width: 8, height: 8, borderRadius: "50%", background: c,
        flexShrink: 0, marginTop: 6,
      }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, color: t.textPrimary, lineHeight: 1.45 }}>
          {article.title}
        </div>
        <div style={{
          display: "flex", gap: 10, marginTop: 4, fontSize: 11,
          color: t.textMuted, fontFamily: "'JetBrains Mono', monospace",
        }}>
          <span>{article.source || "—"}</span>
          <span>·</span>
          <span>{article.published_at ?
            new Date(article.published_at).toISOString().slice(0, 10) : "—"}</span>
        </div>
      </div>
      <span style={{
        fontSize: 10, padding: "2px 8px", borderRadius: 10,
        background: `${c}26`, color: c, textTransform: "uppercase",
        fontFamily: "'JetBrains Mono', monospace", fontWeight: 700,
        letterSpacing: 0.6, flexShrink: 0,
      }}>
        {article.sentiment_label || "—"}
      </span>
    </div>
  );
}

// Beautiful "data unavailable" panel — replaces the plain text we had before.
// Used when news_source is "knowledge_only" or "unavailable".
function KnowledgeOnlyPanel({ d, t }) {
  return (
    <GlassCard t={t} warm padding={22} hoverable={false}>
      <div style={{ display: "flex", gap: 18, alignItems: "flex-start" }}>
        {/* Left: textured icon */}
        <div style={{
          width: 76, height: 76, borderRadius: 14, flexShrink: 0,
          background: `linear-gradient(135deg, ${t.goldSoft} 0%, ${t.surfaceWarm} 100%)`,
          border: `1px solid ${t.borderSoft}`,
          display: "flex", alignItems: "center", justifyContent: "center",
          color: t.gold, position: "relative", overflow: "hidden",
        }}>
          {/* Texture */}
          <div style={{
            position: "absolute", inset: 0,
            backgroundImage: `repeating-linear-gradient(45deg, ${t.gold}11 0 2px, transparent 2px 8px)`,
            opacity: 0.7,
          }} />
          <FileQuestion size={32} style={{ position: "relative", zIndex: 1 }} />
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
            <span style={{
              fontSize: 10, padding: "3px 9px", borderRadius: 12,
              background: t.gold, color: "#fff",
              fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.8,
              fontWeight: 700, textTransform: "uppercase",
            }}>
              {d.news_source === "knowledge_only" ? "Knowledge mode" : "Market+macro mode"}
            </span>
            <span style={{
              fontSize: 11, color: t.textMuted,
              fontFamily: "'JetBrains Mono', monospace"
            }}>
              No news data for this window
            </span>
          </div>
          <div style={{
            fontSize: 18, fontWeight: 700, color: t.textPrimary,
            fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", letterSpacing: -0.3,
            marginBottom: 8,
          }}>
            Direct pre-listing news data is unavailable for this IPO
          </div>
          <p style={{
            margin: "0 0 6px", fontSize: 13, color: t.textSecondary,
            lineHeight: 1.6
          }}>
            Scoring still proceeds using the available market and macro signals.
            The composite below reflects what we could reconstruct from Nifty,
            VIX, and FRED indicators around the listing window.
          </p>
          {(d.groq_summary || d.llama_summary) && (
            <div style={{
              marginTop: 12, padding: "10px 12px", borderRadius: 8,
              background: t.surface, border: `1px solid ${t.borderSoft}`,
              fontSize: 12.5, color: t.textSecondary, lineHeight: 1.55,
            }}>
              <span style={{
                fontSize: 10, fontWeight: 700, color: t.gold,
                fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.6,
                textTransform: "uppercase", marginRight: 6,
              }}>LLM note:</span>
              {d.groq_summary || d.llama_summary}
            </div>
          )}
        </div>
      </div>
    </GlassCard>
  );
}
// ═══════════════════════════════════════════════════════════════════════════
// BOTTOM SECTION COMPONENTS — Macro FRED panel, big charts, data tables
// (these were on v5 and got dropped in the v6 redesign — restoring them)
// ═══════════════════════════════════════════════════════════════════════════

// MacroFREDPanel — global risk + commodity context (CBOE VIX, US 10Y, INR/USD,
// Brent, DXY) plus the regime badges and the LLM macro briefing.
function MacroFREDPanel({ d, t }) {
  const sc = scoreColor(d.macro_score, t);
  return (
    <GlassCard t={t} accent padding={22} hoverable={false}>
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        marginBottom: 16, flexWrap: "wrap", gap: 8,
      }}>
        <CardLabel t={t} icon={<Globe2 size={12} color={t.textMuted} />}>
          Macro Context (FRED) — Global Risk &amp; Commodities
        </CardLabel>
        {d.macro_available ? (
          <span style={{
            fontSize: 11, color: sc, fontFamily: "'JetBrains Mono', monospace",
            fontWeight: 600,
          }}>
            Macro Score: {fmtSign(d.macro_score)}
          </span>
        ) : (
          <span style={{
            fontSize: 10, color: t.textMuted,
            fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.4,
          }}>
            Set FRED_API_KEY to enable
          </span>
        )}
      </div>

      {!d.macro_available ? (
        <div style={{
          color: t.textSecondary, fontStyle: "italic",
          padding: 24, textAlign: "center", fontSize: 13, lineHeight: 1.6,
        }}>
          Macro indicators unavailable — get a free FRED API key at{" "}
          <a href="https://fred.stlouisfed.org/docs/api/api_key.html"
            target="_blank" rel="noreferrer"
            style={{ color: t.blue, textDecoration: "none", fontWeight: 600 }}>
            fred.stlouisfed.org
          </a>{" "}and set <code style={{
            background: t.surfaceWarm, padding: "1px 6px", borderRadius: 4,
            fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
          }}>FRED_API_KEY</code> in your <code style={{
            background: t.surfaceWarm, padding: "1px 6px", borderRadius: 4,
            fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
          }}>.env</code>.
        </div>
      ) : (
        <>
          <div style={{
            display: "grid", gap: 10, marginBottom: 14,
            gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
          }}>
            <MacroCell t={t}
              label="CBOE VIX"
              value={fmt(d.fred_cboe_vix_t1, 1)}
              hint={d.fred_cboe_vix_t1 == null ? ""
                : d.fred_cboe_vix_t1 < 15 ? "Low risk"
                  : d.fred_cboe_vix_t1 > 25 ? "Elevated" : "Moderate"}
              hintColor={d.fred_cboe_vix_t1 == null ? t.textMuted
                : d.fred_cboe_vix_t1 < 15 ? t.accent
                  : d.fred_cboe_vix_t1 > 25 ? t.red : t.textPrimary}
            />
            <MacroCell t={t}
              label="US 10Y Yield"
              value={d.fred_us_10y_yield_t1 != null
                ? `${fmt(d.fred_us_10y_yield_t1, 2)}%` : "—"}
              hint={d.fred_us_10y_yield_t1 == null ? ""
                : d.fred_us_10y_yield_t1 > 4.5 ? "High"
                  : d.fred_us_10y_yield_t1 < 3 ? "Low" : "Normal"}
              hintColor={t.textPrimary}
            />
            <MacroCell t={t}
              label="INR / USD"
              value={d.fred_inr_usd_t1 != null
                ? `₹${fmt(d.fred_inr_usd_t1, 2)}` : "—"}
              hint={d.fred_inr_usd_30d_chg != null
                ? `${fmtPct(d.fred_inr_usd_30d_chg)} (30d)` : ""}
              hintColor={d.fred_inr_usd_30d_chg == null ? t.textMuted
                : d.fred_inr_usd_30d_chg > 0 ? t.red : t.accent}
            />
            <MacroCell t={t}
              label="Brent Crude"
              value={d.fred_oil_brent_t1 != null
                ? `$${fmt(d.fred_oil_brent_t1, 1)}` : "—"}
              hint={d.fred_oil_brent_30d_chg != null
                ? `${fmtPct(d.fred_oil_brent_30d_chg)} (30d)` : ""}
              hintColor={d.fred_oil_brent_30d_chg == null ? t.textMuted
                : d.fred_oil_brent_30d_chg > 0 ? t.red : t.accent}
            />
            <MacroCell t={t}
              label="DXY"
              value={fmt(d.fred_dxy_t1, 1)}
              hint={d.fred_dxy_30d_chg != null
                ? `${fmtPct(d.fred_dxy_30d_chg)} (30d)` : ""}
              hintColor={d.fred_dxy_30d_chg == null ? t.textMuted
                : d.fred_dxy_30d_chg > 0 ? t.red : t.accent}
            />
          </div>

          <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap" }}>
            <RegimeBadge t={t} label="Risk" value={d.macro_risk_regime} />
            <RegimeBadge t={t} label="Rates" value={d.macro_rate_regime} />
            <RegimeBadge t={t} label="Dollar" value={d.macro_dollar_regime} />
          </div>

          {d.macro_briefing && (
            <div style={{
              background: t.surfaceWarm, border: `1px solid ${t.borderSoft}`,
              borderRadius: 10, padding: "13px 16px",
              fontSize: 13, color: t.textPrimary, lineHeight: 1.6,
            }}>
              {d.macro_briefing}
            </div>
          )}
        </>
      )}
    </GlassCard>
  );
}

function MacroCell({ t, label, value, hint, hintColor }) {
  return (
    <div style={{
      background: t.surface, border: `1px solid ${t.borderSoft}`,
      borderRadius: 10, padding: "12px 14px",
      transition: "border-color 0.2s ease, transform 0.2s ease",
    }}
      onMouseEnter={(e) => {
        e.currentTarget.style.borderColor = t.borderStrong;
        e.currentTarget.style.transform = "translateY(-1px)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.borderColor = t.borderSoft;
        e.currentTarget.style.transform = "translateY(0)";
      }}>
      <div style={{
        fontSize: 10, color: t.textMuted, textTransform: "uppercase",
        letterSpacing: 1.2, fontFamily: "'JetBrains Mono', monospace",
        fontWeight: 600, marginBottom: 6,
      }}>{label}</div>
      <div style={{
        fontSize: 20, fontWeight: 700, color: t.textPrimary,
        fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", lineHeight: 1,
        letterSpacing: -0.5, marginBottom: 4,
      }}>{value}</div>
      {hint && (
        <div style={{
          fontSize: 11, color: hintColor || t.textMuted,
          fontFamily: "'JetBrains Mono', monospace", fontWeight: 500,
        }}>{hint}</div>
      )}
    </div>
  );
}

function RegimeBadge({ t, label, value }) {
  const v = (value || "").toLowerCase();
  const tone =
    v.includes("risk-on") || v.includes("bullish") || v.includes("calm") || v.includes("ease")
      ? t.accent
      : v.includes("risk-off") || v.includes("bearish") || v.includes("stress") || v.includes("tight")
        ? t.red
        : t.gold;
  const bg =
    tone === t.accent ? t.accentSoft
      : tone === t.red ? t.redSoft
        : t.goldSoft;
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: 8,
      padding: "6px 12px", borderRadius: 18,
      background: bg, border: `1px solid ${tone}33`,
    }}>
      <span style={{
        fontSize: 9.5, color: t.textMuted,
        fontFamily: "'JetBrains Mono', monospace",
        textTransform: "uppercase", letterSpacing: 1, fontWeight: 600,
      }}>{label}</span>
      <span style={{
        fontSize: 12, color: tone, fontWeight: 700,
        textTransform: "capitalize",
      }}>{value || "—"}</span>
    </div>
  );
}

// DataTable — compact key/value table used for the VIX + Nifty stat blocks
function DataTable({ t, title, rows }) {
  return (
    <GlassCard t={t} padding={18}>
      <CardLabel t={t}>{title}</CardLabel>
      <div style={{ display: "flex", flexDirection: "column", gap: 0, marginTop: 6 }}>
        {rows.map((row, i) => (
          <div key={i} style={{
            display: "flex", alignItems: "center", justifyContent: "space-between",
            padding: "10px 0",
            borderBottom: i < rows.length - 1 ? `1px solid ${t.borderSoft}` : "none",
          }}>
            <span style={{ fontSize: 12.5, color: t.textSecondary }}>
              {row.label}
            </span>
            <span style={{
              fontSize: 13.5, fontWeight: 600,
              color: row.color || t.textPrimary,
              fontFamily: "'JetBrains Mono', monospace",
            }}>
              {row.value}
            </span>
          </div>
        ))}
      </div>
    </GlassCard>
  );
}

// BigAreaChart — full-width recharts area chart with theme-aware colors
function BigAreaChart({ t, title, data, dataKey, color, yFormat,
  tooltipPrefix = "", tooltipFormat = null,
  yDomain = null, height = 240 }) {
  const gradId = `bigChart-${title.replace(/\s/g, "-").toLowerCase()}`;
  return (
    <GlassCard t={t} padding={20}>
      <CardLabel t={t} icon={<TrendingUp size={11} color={t.textMuted} />}>
        {title}
      </CardLabel>
      <div style={{ marginTop: 8 }}>
        <ResponsiveContainer width="100%" height={height}>
          <AreaChart data={data} margin={{ top: 8, right: 16, bottom: 6, left: 6 }}>
            <defs>
              <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={color} stopOpacity={0.32} />
                <stop offset="100%" stopColor={color} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke={t.borderSoft} />
            <XAxis dataKey="label"
              tick={{
                fill: t.textMuted, fontSize: 11,
                fontFamily: "'JetBrains Mono', monospace"
              }}
              axisLine={{ stroke: t.border }} tickLine={false}
              interval={Math.max(1, Math.floor(data.length / 6))} />
            <YAxis tick={{
              fill: t.textMuted, fontSize: 11,
              fontFamily: "'JetBrains Mono', monospace"
            }}
              axisLine={{ stroke: t.border }} tickLine={false}
              tickFormatter={yFormat}
              domain={yDomain || ["dataMin - 100", "dataMax + 100"]} />
            <Tooltip content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null;
              const v = payload[0].value;
              return (
                <div style={{
                  background: t.surfaceSolid,
                  border: `1px solid ${t.border}`, borderRadius: 8,
                  padding: "9px 13px", fontSize: 12,
                  boxShadow: "0 8px 24px rgba(0,0,0,0.12)",
                }}>
                  <div style={{
                    color: t.textMuted, marginBottom: 2,
                    fontFamily: "'JetBrains Mono', monospace"
                  }}>
                    {label}
                  </div>
                  <div style={{
                    color, fontWeight: 700,
                    fontFamily: "'JetBrains Mono', monospace"
                  }}>
                    {tooltipFormat ? tooltipFormat(v) : `${tooltipPrefix}${v}`}
                  </div>
                </div>
              );
            }} />
            <Area type="monotone" dataKey={dataKey} stroke={color}
              strokeWidth={2.4} fill={`url(#${gradId})`} dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </GlassCard>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// SIDEBAR — minimal icon nav (decorative — items don't navigate yet, but
// the structure is here for future expansion)
// ═══════════════════════════════════════════════════════════════════════════
function Sidebar({ t, onToggleTheme, themeName }) {
  const [activeIcon, setActiveIcon] = useState("dashboard");
  const items = [
    { id: "dashboard", icon: <BarChart3 size={18} />, label: "Dashboard" },
    { id: "saved", icon: <Bookmark size={17} />, label: "Saved" },
    { id: "history", icon: <History size={17} />, label: "History" },
    { id: "starred", icon: <Star size={17} />, label: "Starred" },
  ];
  return (
    <aside style={{
      width: 56, flexShrink: 0,
      display: "flex", flexDirection: "column", alignItems: "center",
      padding: "20px 0", gap: 4,
      borderRight: `1px solid ${t.borderSoft}`,
      background: t.surface,
      backdropFilter: "blur(12px)",
      WebkitBackdropFilter: "blur(12px)",
    }}>
      {/* Bull logo */}
      <div style={{
        width: 32, height: 32, marginBottom: 16,
        display: "flex", alignItems: "center", justifyContent: "center",
        color: t.accent,
      }}>
        <BullLogo size={26} color={t.accent} />
      </div>
      {items.map((item) => (
        <SidebarButton key={item.id} t={t} active={activeIcon === item.id}
          onClick={() => setActiveIcon(item.id)}
          title={item.label}>
          {item.icon}
        </SidebarButton>
      ))}
      <div style={{ flex: 1 }} />
      {/* Theme toggle pinned to bottom */}
      <SidebarButton t={t} onClick={onToggleTheme}
        title={`Switch to ${themeName === "light" ? "dark" : "light"} mode`}>
        {themeName === "light" ? <Moon size={17} /> : <Sun size={17} />}
      </SidebarButton>
      <SidebarButton t={t} title="Settings">
        <Settings size={17} />
      </SidebarButton>
    </aside>
  );
}

function SidebarButton({ t, active = false, onClick, children, title }) {
  const [hover, setHover] = useState(false);
  return (
    <button onClick={onClick} title={title}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{
        width: 38, height: 38, border: "none", borderRadius: 10,
        cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
        background: active ? t.accentSoft : (hover ? t.surfaceHover : "transparent"),
        color: active ? t.accent : (hover ? t.textPrimary : t.textMuted),
        transition: "all 0.2s ease",
      }}>
      {children}
    </button>
  );
}

// Inline SVG bull silhouette (logo)
function BullLogo({ size = 24, color = "currentColor" }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none">
      <path d="M6 11 C6 8 8 6 11 7 L13 9 L19 9 L21 7 C24 6 26 8 26 11 L26 14 C26 18 23 22 19 23 L13 23 C9 22 6 18 6 14 Z"
        fill={color} opacity={0.85} />
      <path d="M3 9 L7 11 M29 9 L25 11" stroke={color} strokeWidth={1.6}
        strokeLinecap="round" fill="none" />
      <circle cx="13" cy="14" r="1" fill="#fff" opacity={0.85} />
      <circle cx="19" cy="14" r="1" fill="#fff" opacity={0.85} />
      <path d="M14 18 L16 20 L18 18" stroke="#fff" strokeWidth={1.2}
        strokeLinecap="round" strokeLinejoin="round" fill="none" opacity={0.9} />
    </svg>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// PULSING LIVE INDICATOR
// ═══════════════════════════════════════════════════════════════════════════
function LiveIndicator({ t, label = "Live" }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <span style={{
        position: "relative", width: 8, height: 8, borderRadius: "50%",
        background: t.accent,
      }}>
        <span style={{
          position: "absolute", inset: -3, borderRadius: "50%",
          background: t.accent, opacity: 0.5,
          animation: "ipoPulse 1.8s cubic-bezier(0,0,.2,1) infinite",
        }} />
      </span>
      <span style={{
        fontSize: 12, color: t.accent, fontFamily: "'JetBrains Mono', monospace",
        fontWeight: 600, letterSpacing: 0.5,
      }}>
        {label}
      </span>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════════════════
export default function SentimentTerminal() {
  // Theme — persisted to localStorage across sessions
  const [themeName, setThemeName] = useState(() => {
    if (typeof window !== "undefined") {
      const saved = window.localStorage.getItem("ipo_theme");
      if (saved === "light" || saved === "dark") return saved;
      // Auto: respect prefers-color-scheme
      if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) return "dark";
    }
    return "light";
  });
  const t = THEMES[themeName];
  useEffect(() => {
    try { window.localStorage.setItem("ipo_theme", themeName); } catch { /* ignore */ }
  }, [themeName]);
  const toggleTheme = () => setThemeName((n) => n === "light" ? "dark" : "light");

  // Score-weight visibility toggles (cosmetic — like the target screenshot)
  const [weightsVisible, setWeightsVisible] = useState({
    groqLlama: true, finbert: true, macro: true, momentum: true, readability: true,
  });

  // App state
  const [query, setQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [selectedIPO, setSelectedIPO] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [liveAnalyze, setLiveAnalyze] = useState(null);
  const searchRef = useRef(null);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e) => {
      if (searchRef.current && !searchRef.current.contains(e.target)) setShowDropdown(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // ─── Search ─────────────────────────────────────────────────────────
  const doSearch = useCallback(async (q) => {
    try {
      const res = await fetch(`${API_BASE}/api/search?q=${encodeURIComponent(q)}&limit=10&type=ipo`);
      if (res.ok) {
        const d = await res.json();
        if ((d.results || []).length > 0) { setSearchResults(d.results); return; }
      }
    } catch { /* network error — fall through */ }
    if (USE_MOCK_FALLBACK) {
      const filtered = MOCK_SEARCH_RESULTS.filter(r =>
        r.display_label.toLowerCase().includes((q || "").toLowerCase())
      );
      setSearchResults(filtered.length ? filtered : (q ? [] : MOCK_SEARCH_RESULTS));
      return;
    }
    setSearchResults([]);
  }, []);
  useEffect(() => { doSearch(query); }, [query, doSearch]);

  // ─── Load sentiment ─────────────────────────────────────────────────
  const loadSentiment = useCallback(async (ipo, allowCustom = false) => {
    if (!ipo) return;
    setLoading(true); setError(null); setData(null);
    setSelectedIPO(ipo); setShowDropdown(false); setQuery("");
    try {
      const customParam = allowCustom ? "&allow_custom=true" : "";
      const url = `${API_BASE}/api/sentiment/${encodeURIComponent(ipo.ipo_name)}?listing_date=${ipo.listing_date}${customParam}`;
      const res = await fetch(url);
      if (res.status === 404) {
        const body = await res.json().catch(() => ({}));
        const detail = body.detail || body;
        setError({
          kind: "not_found", message: detail.error || "IPO not found",
          hint: detail.hint, canForce: !allowCustom
        });
        setLoading(false); return;
      }
      if (res.status === 400) {
        const body = await res.json().catch(() => ({}));
        const detail = body.detail || body;
        setError({
          kind: "not_found", message: detail.error || "Invalid request",
          hint: detail.expected ? `Expected: ${detail.expected}` : null
        });
        setLoading(false); return;
      }
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        const detail = body.detail || body;
        setError({ kind: "pipeline", message: detail.reason || detail.error || `HTTP ${res.status}` });
        setLoading(false); return;
      }
      setData(await res.json());
    } catch {
      if (USE_MOCK_FALLBACK) {
        setData(generateMockSentiment(ipo.ipo_name, ipo.listing_date));
      } else {
        setError({
          kind: "network",
          message: `Could not reach the API server at ${API_BASE}. Make sure the backend is running.`
        });
      }
    } finally { setLoading(false); }
  }, []);

  // Inline analyze (for IPOs not in CSV)
  const handleAnalyzeDirect = (preName) => {
    const name = (preName || query).trim();
    if (!name) return;
    setShowDropdown(false);
    setLiveAnalyze({ name, date: new Date().toISOString().slice(0, 10) });
  };
  const submitLiveAnalyze = () => {
    if (!liveAnalyze) return;
    const name = liveAnalyze.name.trim();
    const date = liveAnalyze.date;
    if (!name || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return;
    setLiveAnalyze(null);
    loadSentiment({
      ipo_name: name, listing_date: date,
      display_label: `${name} (${date})`
    }, true);
  };

  // Auto-load first IPO on mount
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/search?q=&limit=1`);
        if (res.ok) {
          const d = await res.json();
          if (d.results?.length) { loadSentiment(d.results[0]); return; }
        }
      } catch { /* fall through */ }
      if (USE_MOCK_FALLBACK) loadSentiment(MOCK_SEARCH_RESULTS[0]);
    })();
  }, [loadSentiment]);

  const reset = () => {
    setError(null); setData(null); setSelectedIPO(null);
    setQuery(""); setShowDropdown(true);
  };
  const retry = () => selectedIPO && loadSentiment(selectedIPO);
  const forceAnalyze = () => selectedIPO && loadSentiment(selectedIPO, true);

  const d = data;
  const sentimentDist = d ? {
    pos: d.avg_positive ?? 0, neg: d.avg_negative ?? 0, neu: d.avg_neutral ?? 0,
  } : null;

  return (
    <div style={{
      fontFamily: "'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif",
      background: t.bgGradient,
      backgroundAttachment: "fixed",
      color: t.textPrimary, minHeight: "100vh",
      display: "flex",
      transition: "background 0.4s ease, color 0.4s ease",
    }}>
      {/* Fonts + global keyframes */}
      <link rel="preconnect" href="https://fonts.googleapis.com" />
      <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
      <link
        href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,400..800;1,9..144,400..700&family=IBM+Plex+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&family=Instrument+Serif:ital@0;1&display=swap"
        rel="stylesheet"
      />
      <style>{`
        @keyframes ipoPulse {
          0% { transform: scale(0.85); opacity: 0.7; }
          80%, 100% { transform: scale(2.4); opacity: 0; }
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes fadeUp {
          from { opacity: 0; transform: translateY(8px); }
          to { opacity: 1; transform: translateY(0); }
        }
        ::selection { background: ${t.accentSoft}; color: ${t.textPrimary}; }
        body { margin: 0; font-feature-settings: "ss01", "cv11"; }
        input, button { font-family: inherit; }
        /* Smooth scrolling for the articles list and macro cell hover */
        * { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }
      `}</style>

      <Sidebar t={t} onToggleTheme={toggleTheme} themeName={themeName} />

      {/* Pattern overlay (subtle topographic dots) */}
      <div style={{
        position: "fixed", inset: 0, pointerEvents: "none",
        backgroundImage: t.bgPattern, backgroundSize: "60px 60px",
        opacity: 0.55, zIndex: 0,
      }} />

      <main style={{
        flex: 1, padding: "20px 28px 40px", maxWidth: 1480, margin: "0 auto",
        position: "relative", zIndex: 1, animation: "fadeUp 0.5s ease",
      }}>
        {/* ─── HEADER ─── */}
        <header style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          gap: 16, marginBottom: 22,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <BullLogo size={22} color={t.accent} />
            <span style={{
              fontSize: 13, fontWeight: 700, color: t.accent,
              textTransform: "uppercase", letterSpacing: 2.2,
              fontFamily: "'JetBrains Mono', monospace",
            }}>
              Sentiment Terminal
            </span>
          </div>

          {/* Search bar */}
          <div ref={searchRef} style={{ position: "relative", flex: "0 1 460px" }}>
            <div style={{
              display: "flex", alignItems: "center",
              background: t.surface,
              backdropFilter: "blur(12px)",
              WebkitBackdropFilter: "blur(12px)",
              border: `1px solid ${showDropdown ? t.borderStrong : t.border}`,
              borderRadius: 10, padding: "9px 14px", gap: 10, cursor: "text",
              transition: "border-color 0.2s ease, box-shadow 0.2s ease",
              boxShadow: showDropdown ? t.glow : t.shadowResting,
            }} onClick={() => setShowDropdown(true)}>
              <Search size={16} color={t.textMuted} />
              {selectedIPO && !showDropdown ? (
                <span style={{ flex: 1, fontSize: 14.5, fontWeight: 500, color: t.textPrimary }}>
                  {selectedIPO.display_label || selectedIPO.ipo_name}
                </span>
              ) : (
                <input value={query}
                  onChange={(e) => { setQuery(e.target.value); setShowDropdown(true); }}
                  onKeyDown={(e) => { if (e.key === "Enter" && query.trim()) handleAnalyzeDirect(query); }}
                  placeholder="Search IPO or type any name..."
                  onFocus={() => setShowDropdown(true)}
                  style={{
                    flex: 1, background: "transparent", border: "none", outline: "none",
                    color: t.textPrimary, fontSize: 14.5,
                  }} />
              )}
              <ChevronDown size={15} color={t.textMuted}
                style={{
                  transform: showDropdown ? "rotate(180deg)" : "rotate(0)",
                  transition: "transform 0.2s ease"
                }} />
            </div>

            {/* Dropdown */}
            {showDropdown && (() => {
              const ipoOnly = searchResults.filter(r =>
                r.result_type === "ipo" || /\(\w+ \d{4}\)/.test(r.display_label || "")
              );
              const liveResults = searchResults.filter(r => r.result_type === "live_analyze");
              return (
                <div style={{
                  position: "absolute", top: "calc(100% + 6px)", left: 0, right: 0,
                  background: t.surface,
                  backdropFilter: "blur(16px)",
                  WebkitBackdropFilter: "blur(16px)",
                  border: `1px solid ${t.border}`, borderRadius: 12,
                  maxHeight: 380, overflowY: "auto", zIndex: 50,
                  boxShadow: "0 18px 40px -8px rgba(0,0,0,0.18)",
                  animation: "fadeUp 0.18s ease",
                }}>
                  {ipoOnly.map((r, i) => (
                    <DropdownItem key={i} t={t}
                      onClick={() => loadSentiment(r)}
                      label={r.display_label}
                      badge="IPO" badgeColor={t.accent}
                      trailing={r.listing_gain != null && (
                        <span style={{
                          fontSize: 12, fontWeight: 600,
                          color: r.listing_gain > 0 ? t.accent : t.red,
                          fontFamily: "'JetBrains Mono', monospace",
                        }}>
                          {r.listing_gain > 0 ? "+" : ""}{r.listing_gain.toFixed(1)}%
                        </span>
                      )} />
                  ))}
                  {(query.trim().length > 1 || liveResults.length > 0) && (
                    <div onClick={() => handleAnalyzeDirect(query || (liveResults[0] && liveResults[0].ipo_name))}
                      onMouseEnter={(e) => e.currentTarget.style.background = t.goldSoft}
                      onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}
                      style={{
                        padding: "13px 16px", cursor: "pointer",
                        display: "flex", alignItems: "center", gap: 12,
                        borderTop: ipoOnly.length > 0 ? `1px solid ${t.borderSoft}` : "none",
                        transition: "background 0.15s ease",
                      }}>
                      <div style={{
                        width: 28, height: 28, borderRadius: 7,
                        background: t.gold, color: "#fff",
                        display: "flex", alignItems: "center", justifyContent: "center",
                      }}>
                        <Zap size={14} />
                      </div>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 13.5, color: t.gold, fontWeight: 700 }}>
                          Analyze "{(query || (liveResults[0] && liveResults[0].ipo_name) || "").trim()}"
                        </div>
                        <div style={{ fontSize: 11, color: t.textMuted, marginTop: 1 }}>
                          Run live pipeline — pick a listing date
                        </div>
                      </div>
                    </div>
                  )}
                  {ipoOnly.length === 0 && query.trim().length <= 1 && (
                    <div style={{
                      padding: 18, textAlign: "center",
                      fontSize: 12.5, color: t.textMuted,
                    }}>
                      Type to search the IPO master, or any name to analyze live.
                    </div>
                  )}
                </div>
              );
            })()}
          </div>

          {/* Right cluster: Live indicator + theme toggle */}
          <div style={{ display: "flex", alignItems: "center", gap: 18 }}>
            <LiveIndicator t={t} />
          </div>
        </header>

        {/* ─── LOADING ─── */}
        {loading && (
          <GlassCard t={t} hoverable={false} padding={60} style={{ textAlign: "center", marginTop: 24 }}>
            <Loader2 size={28} color={t.accent}
              style={{ animation: "spin 1s linear infinite", marginBottom: 14 }} />
            <div style={{
              color: t.textSecondary, fontSize: 14,
              fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.4
            }}>
              Loading sentiment data...
            </div>
          </GlassCard>
        )}

        {/* ─── ERROR STATES ─── */}
        {!loading && error?.kind === "not_found" && (
          <NotFoundScreen t={t}
            ipoName={selectedIPO?.display_label || selectedIPO?.ipo_name}
            listingDate={selectedIPO?.listing_date}
            hint={error.hint}
            onReset={reset}
            onForceAnalyze={error.canForce ? forceAnalyze : undefined} />
        )}
        {!loading && (error?.kind === "pipeline" || error?.kind === "network") && (
          <ErrorScreen t={t} message={error.message} onRetry={retry} />
        )}

        {/* ─── DATA — full dashboard grid ─── */}
        {!loading && !error && d && (
          <DashboardGrid d={d} t={t} sentimentDist={sentimentDist}
            weightsVisible={weightsVisible}
            setWeightsVisible={setWeightsVisible} />
        )}

        {/* Inline live-analyze modal */}
        {liveAnalyze && (
          <LiveAnalyzeModal t={t} state={liveAnalyze}
            onChange={setLiveAnalyze}
            onSubmit={submitLiveAnalyze}
            onClose={() => setLiveAnalyze(null)} />
        )}

        <footer style={{
          textAlign: "center", marginTop: 30,
          fontSize: 11, color: t.textMuted, opacity: 0.7,
          fontFamily: "'JetBrains Mono', monospace", letterSpacing: 0.6,
        }}>
          Source Data &amp; Models <span style={{ marginLeft: 4 }}>↗</span>
        </footer>
      </main>
    </div>
  );
}

function DropdownItem({ t, onClick, label, badge, badgeColor, trailing }) {
  const [hover, setHover] = useState(false);
  return (
    <div onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        padding: "11px 16px", cursor: "pointer",
        display: "flex", alignItems: "center", gap: 10,
        background: hover ? t.surfaceHover : "transparent",
        borderBottom: `1px solid ${t.borderSoft}`,
        transition: "background 0.15s ease",
      }}>
      <span style={{
        fontSize: 9, color: badgeColor,
        border: `1px solid ${badgeColor}`,
        borderRadius: 4, padding: "1px 6px",
        fontFamily: "'JetBrains Mono', monospace", fontWeight: 700,
        letterSpacing: 0.6,
      }}>{badge}</span>
      <span style={{ flex: 1, fontSize: 13.5, color: t.textPrimary }}>{label}</span>
      {trailing}
    </div>
  );
}

function LiveAnalyzeModal({ t, state, onChange, onSubmit, onClose }) {
  return (
    <div onClick={onClose} style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,0.42)",
      backdropFilter: "blur(4px)", WebkitBackdropFilter: "blur(4px)",
      display: "flex", alignItems: "center", justifyContent: "center",
      zIndex: 100, animation: "fadeUp 0.2s ease",
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        background: t.surfaceSolid,
        border: `1px solid ${t.borderStrong}`, borderRadius: 16,
        padding: "30px 34px", width: "min(460px, 92vw)",
        boxShadow: "0 30px 60px rgba(0,0,0,0.25)",
      }}>
        <div style={{
          display: "inline-flex", alignItems: "center", gap: 6,
          background: t.goldSoft, color: t.gold,
          padding: "4px 10px", borderRadius: 16, marginBottom: 12,
          fontSize: 10, fontWeight: 700, letterSpacing: 1.2,
          fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase",
        }}>
          <Zap size={11} /> Live Analyze
        </div>
        <div style={{
          fontSize: 22, fontWeight: 700, color: t.textPrimary,
          fontFamily: "'Fraunces', 'Instrument Serif', Georgia, serif", marginBottom: 6,
          letterSpacing: -0.4,
        }}>
          Run pipeline for any IPO
        </div>
        <p style={{
          margin: "0 0 22px", fontSize: 13, color: t.textSecondary,
          lineHeight: 1.55
        }}>
          Bypasses the historical CSV. Use this for IPOs not in the master
          spreadsheet, including upcoming or recently-listed issues.
        </p>

        <FieldLabel t={t}>Company name</FieldLabel>
        <input type="text" value={state.name}
          onChange={(e) => onChange({ ...state, name: e.target.value })}
          style={modalInputStyle(t)} />

        <FieldLabel t={t} style={{ marginTop: 16 }}>Listing date</FieldLabel>
        <input type="date" value={state.date}
          onChange={(e) => onChange({ ...state, date: e.target.value })}
          onKeyDown={(e) => { if (e.key === "Enter") onSubmit(); }}
          style={{ ...modalInputStyle(t), fontFamily: "'JetBrains Mono', monospace" }} />

        <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", marginTop: 24 }}>
          <button onClick={onClose} style={{
            background: "transparent", border: `1px solid ${t.border}`,
            color: t.textSecondary, padding: "10px 20px", borderRadius: 8,
            cursor: "pointer", fontSize: 13.5, fontWeight: 500,
          }}>Cancel</button>
          <button onClick={onSubmit} style={{
            background: t.gold, border: "none", color: "#fff",
            padding: "10px 24px", borderRadius: 8, cursor: "pointer",
            fontSize: 13.5, fontFamily: "'JetBrains Mono', monospace",
            fontWeight: 700, letterSpacing: 0.5,
          }}>Analyze →</button>
        </div>
      </div>
    </div>
  );
}

function FieldLabel({ children, t, style = {} }) {
  return (
    <label style={{
      display: "block", fontSize: 10.5, color: t.textMuted,
      textTransform: "uppercase", letterSpacing: 1.4, marginBottom: 6,
      fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
      ...style,
    }}>{children}</label>
  );
}

function modalInputStyle(t) {
  return {
    width: "100%", boxSizing: "border-box", padding: "11px 13px",
    border: `1px solid ${t.border}`, borderRadius: 8, fontSize: 14,
    background: t.surface, color: t.textPrimary, outline: "none",
  };
}