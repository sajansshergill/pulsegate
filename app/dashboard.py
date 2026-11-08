"""PulseGate — Streamlit + Plotly console for the event-contract quality gate.

Demo data is deterministic and derived directly from the project's own
schemas: the 7 versioned contracts in contracts/registry/*.v1.json
(session_start, post_create, like, follow, message_send, ad_impression,
ad_click), the 6 surfaces from generator/generate.py, the fault kinds from
generator.corrupt (drop_field, wrong_type, extra_field, bad_enum) and the
detectors / OWNER_MAP / severities from quality/detectors.py + quality/triage.py.
The app runs standalone from repo contents — no warehouse or Kafka needed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="PulseGate", page_icon="🛡️", layout="wide")

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling (mirrors FlowScope)
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 12% -10%, #0b3d2e 0%, transparent 55%),
                    radial-gradient(1000px 500px at 92% 0%, #2b1158 0%, transparent 52%),
                    radial-gradient(800px 400px at 50% 110%, #0e2a4a 0%, transparent 55%),
                    #0e1117;
    }
    .pg-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(16,120,80,.55), rgba(70,40,180,.48), rgba(14,80,140,.45));
        border: 1px solid rgba(110,255,190,.32);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .pg-hero h1 {
        margin: 0; font-size: 2.05rem; letter-spacing: .4px;
        background: linear-gradient(90deg,#8fffc9,#9fd0ff,#e3b3ff);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .pg-hero p { margin: .25rem 0 0 0; color: #cfd8e3; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,.88); border: 1px solid rgba(110,255,190,.30);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(227,179,255,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,34,.85); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(14,17,23,.94); }
    .pg-pill {
        display:inline-block; padding:.15rem .55rem; border-radius:999px;
        border:1px solid rgba(255,255,255,.25); font-size:.78rem; margin-right:.35rem;
    }
    </style>
    <div class="pg-hero">
      <h1>🛡️ PulseGate — Event-Contract Quality Gate</h1>
      <p>Versioned event contracts enforced at producer &amp; ingestion · bronze → silver → gold ·
         volume drops, null spikes, schema drift &amp; duplicates triaged with severity and owner.
         Drag the 3D quality space, press play on the animated charts, slice everything from the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# Domain constants — copied from the project's own code
# ----------------------------------------------------------------------------
EVENT_TYPES = [
    "session_start", "post_create", "like", "follow",
    "message_send", "ad_impression", "ad_click",
]
SURFACES = ["feed", "reels", "stories", "messaging", "ads", "notifications"]
FAULT_KINDS = ["drop_field", "wrong_type", "extra_field", "bad_enum"]
# quality/triage.py OWNER_MAP
OWNER_MAP = {
    "volume_anomaly": "ingest-oncall",
    "null_rate": "producer-team",
    "duplicate_event": "ingest-oncall",
    "dead_letter_rate": "producer-team",
    "schema_drift": "producer-team",
    "referential_integrity": "analytics-eng",
}
BASE_VOLUME = {
    "session_start": 42000, "post_create": 28500, "like": 186000,
    "follow": 34200, "message_send": 96500, "ad_impression": 212000, "ad_click": 18900,
}
# Plausible surface affinity per event type (rows sum ~1)
SURFACE_WEIGHT = {
    "session_start": [0.22, 0.24, 0.18, 0.14, 0.06, 0.16],
    "post_create": [0.38, 0.28, 0.20, 0.03, 0.02, 0.09],
    "like": [0.34, 0.36, 0.18, 0.03, 0.04, 0.05],
    "follow": [0.30, 0.26, 0.16, 0.06, 0.04, 0.18],
    "message_send": [0.06, 0.04, 0.05, 0.72, 0.02, 0.11],
    "ad_impression": [0.32, 0.30, 0.20, 0.02, 0.14, 0.02],
    "ad_click": [0.33, 0.29, 0.19, 0.02, 0.15, 0.02],
}


def _load_contracts() -> pd.DataFrame:
    """Read the real versioned contracts from contracts/registry/ if present."""
    reg_dir = Path(__file__).resolve().parents[1] / "contracts" / "registry"
    rows = []
    if reg_dir.exists():
        for p in sorted(reg_dir.glob("*.json")):
            try:
                s = json.loads(p.read_text())
                props = s.get("properties", {})
                rows.append({
                    "event_type": p.name.split(".v")[0],
                    "version": 1,
                    "required_fields": len(s.get("required", [])),
                    "total_fields": len(props),
                    "fields": ", ".join(sorted(props.keys())),
                    "additional_properties": str(s.get("additionalProperties", True)),
                })
            except Exception:
                continue
    if not rows:  # fallback — the 7 v1 contracts as shipped
        fallback = {
            "ad_click": 9, "ad_impression": 9, "follow": 7, "like": 8,
            "message_send": 9, "post_create": 9, "session_start": 9,
        }
        rows = [{"event_type": k, "version": 1, "required_fields": v, "total_fields": v,
                 "fields": "", "additional_properties": "False"} for k, v in fallback.items()]
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_demo() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Deterministic demo data (seed 42) derived from the project's schemas.

    Returns (daily, hourly_gate, triage):
      daily       — one row per date × event_type × surface
      hourly_gate — bronze→silver→gold gate checks per day
      triage      — triage records in quality.triage.TriageRecord shape
    """
    rng = np.random.default_rng(42)
    dates = pd.date_range("2026-10-18", "2026-11-07", freq="D")  # 21 days
    rows = []
    for di, d in enumerate(dates):
        dow = d.weekday()
        weekend = 1.12 if dow >= 5 else 1.0
        trend = 1.0 + di * 0.004
        for et in EVENT_TYPES:
            base = BASE_VOLUME[et] * weekend * trend
            for si, surf in enumerate(SURFACES):
                w = SURFACE_WEIGHT[et][si]
                vol = int(base * w * rng.normal(1.0, 0.06))
                # --- injected incidents (the story the detectors tell) ---
                null_rate = float(np.clip(rng.normal(0.008, 0.004), 0, 0.05))
                reject_rate = float(np.clip(rng.normal(0.011, 0.005), 0.001, 0.04))
                dup = int(max(0, rng.normal(3, 2)))
                drift = int(max(0, rng.normal(2, 1.5)))
                if et == "like" and surf == "reels" and pd.Timestamp("2026-11-02") <= d <= pd.Timestamp("2026-11-04"):
                    vol = int(vol * 0.38)  # volume drop — volume_anomaly / critical
                if et == "message_send" and d == pd.Timestamp("2026-11-03"):
                    null_rate = 0.31  # null spike on user_id — null_rate / critical
                    reject_rate = 0.09
                if et == "session_start" and d == pd.Timestamp("2026-11-05"):
                    drift = 214  # extra_field storm — schema_drift / critical
                    reject_rate = 0.16
                if d == pd.Timestamp("2026-11-01"):
                    dup = 148  # duplicate_event / critical
                if et == "ad_impression" and d == pd.Timestamp("2026-10-29"):
                    vol = int(vol * 1.55)  # volume spike
                rejected = int(vol * reject_rate / max(1 - reject_rate, 0.01)) if reject_rate < 1 else 0
                total_attempted = vol + rejected
                rows.append({
                    "date": d, "event_type": et, "surface": surf,
                    "volume": vol, "attempted": total_attempted, "rejected": rejected,
                    "reject_rate": rejected / total_attempted if total_attempted else 0.0,
                    "null_rate": null_rate, "duplicate_count": dup,
                    "drift_count": drift,
                    "p95_latency_ms": float(np.clip(rng.normal(118, 22), 62, 240)),
                })
    daily = pd.DataFrame(rows)

    # Gate checks bronze → silver → gold per day (pass rates)
    gate_rows = []
    for d in dates:
        day = daily[daily["date"] == d]
        br = 1 - float(day["rejected"].sum() / max(day["attempted"].sum(), 1))
        gate_rows.append({
            "date": d, "layer": "bronze", "pass_rate": round(br, 4),
            "checks": 7, "failed_checks": int(br < 0.97),
            "detail": "contract validation at ingestion (ContractRegistry.validate)",
        })
        sil = float(np.clip(br + rng.normal(0.012, 0.004), 0, 0.999))
        gate_rows.append({
            "date": d, "layer": "silver", "pass_rate": round(sil, 4),
            "checks": 12, "failed_checks": int(sil < 0.985),
            "detail": "dedup on event_id, null gate on event_id/user_id/surface/occurred_at",
        })
        gold = float(np.clip(sil + rng.normal(0.006, 0.003), 0, 0.9995))
        gate_rows.append({
            "date": d, "layer": "gold", "pass_rate": round(gold, 4),
            "checks": 9, "failed_checks": int(gold < 0.995),
            "detail": "reconciles to bronze within 0.1% · SLA freshness ≤2h",
        })
    gate = pd.DataFrame(gate_rows)

    triage_data = [
        ("a91f0c2e41b7", "volume_anomaly", "volume_dropped", "critical", "like",
         "like volume dropped 62% vs baseline on reels (2026-11-02 → 11-04)", "ingest-oncall", "2026-11-02", 14),
        ("b7c41d09e2aa", "null_rate", "null_spike", "critical", "message_send",
         "message_send.user_id null rate up 31% — producer build 8.4.1", "producer-team", "2026-11-03", 9),
        ("c03e88aa19f4", "schema_drift", "unexpected_field", "critical", None,
         "214 event(s) rejected for fields outside the contract (debug_flag)", "producer-team", "2026-11-05", 6),
        ("d5519be7c930", "duplicate_event", "duplicate_event_id", "critical", None,
         "148 duplicate event_id(s) in bronze — retry storm, no idempotency key", "ingest-oncall", "2026-11-01", 11),
        ("e812aa04d771", "dead_letter_rate", "reject_rate_up", "warning", None,
         "dead-letter rate 8.9% vs baseline 1.1% (extra_field + drop_field)", "producer-team", "2026-11-05", 5),
        ("f2046c91ab08", "volume_anomaly", "volume_spiked", "warning", "ad_impression",
         "ad_impression volume spiked 55% vs baseline on 2026-10-29", "ingest-oncall", "2026-10-29", 3),
        ("07aa91c3d5e2", "null_rate", "null_spike", "warning", "session_start",
         "session_start.surface null rate up 7% after app_version 9.0 rollout", "producer-team", "2026-10-31", 4),
        ("19d3f7a08bc1", "referential_integrity", "orphaned_event", "warning", "ad_click",
         "312 ad_click without matching ad_impression (2.8%)", "analytics-eng", "2026-11-06", 2),
        ("2ac901ee47d9", "schema_drift", "unexpected_field", "info", "post_create",
         "11 event(s) rejected for fields outside the contract", "producer-team", "2026-10-24", 1),
    ]
    triage = pd.DataFrame(triage_data, columns=[
        "fingerprint", "detector", "signal", "severity", "event_type",
        "detail", "suggested_owner", "first_seen", "seen_count",
    ])
    triage["first_seen"] = pd.to_datetime(triage["first_seen"])
    triage["event_type"] = triage["event_type"].fillna("— all types —")
    return daily, gate, triage


contracts = _load_contracts()
daily, gate, triage = load_demo()

# ----------------------------------------------------------------------------
# Sidebar — global dynamic filters (every view below reads filtered data)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
dmin, dmax = daily["date"].min(), daily["date"].max()
picked = st.sidebar.date_input(
    "Date range", value=(dmin.date(), dmax.date()),
    min_value=dmin.date(), max_value=dmax.date(),
)
if isinstance(picked, (list, tuple)) and len(picked) == 2:
    start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
else:
    start, end = dmin, dmax
sel_events = st.sidebar.multiselect("Event types (contracts)", EVENT_TYPES, default=EVENT_TYPES)
sel_surfaces = st.sidebar.multiselect("Surfaces", SURFACES, default=SURFACES)
sel_sev = st.sidebar.multiselect("Triage severity", ["critical", "warning", "info"],
                                 default=["critical", "warning", "info"])
min_vol = st.sidebar.slider("Min daily volume per slice", 0, 50000, 0, 1000,
                            help="Hide tiny event × surface slices")
show_rejected = st.sidebar.toggle("Include dead-letter (rejected) in volume", value=True)

f_daily = daily[
    (daily["date"] >= start) & (daily["date"] <= end)
    & daily["event_type"].isin(sel_events or EVENT_TYPES)
    & daily["surface"].isin(sel_surfaces or SURFACES)
    & (daily["volume"] >= min_vol)
].copy()
f_daily["display_volume"] = f_daily["volume"] + (f_daily["rejected"] if show_rejected else 0)
f_gate = gate[(gate["date"] >= start) & (gate["date"] <= end)].copy()
f_triage = triage[
    (triage["first_seen"] >= start) & (triage["first_seen"] <= end)
    & triage["severity"].isin(sel_sev or ["critical", "warning", "info"])
].copy()
if sel_events:
    f_triage = f_triage[f_triage["event_type"].isin(list(sel_events) + ["— all types —"])]

st.sidebar.markdown("---")
st.sidebar.caption(
    f"**{len(contracts)} contracts** · v1 · `additionalProperties: false`  \n"
    f"Detectors: volume_anomaly · null_rate · duplicate_event · "
    f"dead_letter_rate · schema_drift · referential_integrity"
)

# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period
# ----------------------------------------------------------------------------
if not f_daily.empty:
    total_vol = int(f_daily["display_volume"].sum())
    total_att = int(f_daily["attempted"].sum())
    pass_rate = float(f_daily["volume"].sum() / max(total_att, 1))
    dl_rate = 1 - pass_rate
    span = max((end - start).days, 1)
    prior = daily[(daily["date"] >= start - pd.Timedelta(days=span + 1))
                  & (daily["date"] < start)
                  & daily["event_type"].isin(sel_events or EVENT_TYPES)
                  & daily["surface"].isin(sel_surfaces or SURFACES)]
    prior_vol = int(prior["display_vol"].sum()) if "display_vol" in prior else int(prior["volume"].sum() + prior["rejected"].sum()) if not prior.empty else 0
    prior_pass = float(prior["volume"].sum() / max(prior["attempted"].sum(), 1)) if not prior.empty else pass_rate
    crit = int((f_triage["severity"] == "critical").sum())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Events through the gate", f"{total_vol:,}",
              delta=f"{total_vol - prior_vol:+,} vs prior period" if not prior.empty else None)
    c2.metric("Contract pass rate", f"{pass_rate:.2%}",
              delta=f"{(pass_rate - prior_pass) * 100:+.2f} pts vs prior")
    c3.metric("Dead-letter rate", f"{dl_rate:.2%}",
              delta=f"{int(f_daily['rejected'].sum()):,} rejected → dead_letter", delta_color="inverse")
    c4.metric("Open critical triage", f"{crit}",
              delta=f"{len(f_triage)} total open · {f_triage['suggested_owner'].nunique() if not f_triage.empty else 0} owners")
    c5.metric("Contracts enforced", f"{len(contracts)} × v1",
              delta=f"{int(f_daily['duplicate_count'].sum()):,} duplicates caught")

tab_gate, tab_anim, tab_3d, tab_contract, tab_triage = st.tabs([
    "🚪 Gate overview — bronze → gold",
    "🎬 Animated volume",
    "🧊 3D quality space",
    "📜 Contract health",
    "🚨 Triage queue",
])

# ----------------------------------------------------------------------------
with tab_gate:
    st.subheader("Gate checks — bronze → silver → gold")
    if f_gate.empty or f_daily.empty:
        st.info("No rows in the current filter.")
    else:
        fig = px.line(f_gate.sort_values("date"), x="date", y="pass_rate", color="layer",
                      markers=True, template="plotly_dark",
                      labels={"pass_rate": "Pass rate", "date": "Date", "layer": "Layer"},
                      title="Pass rate by medallion layer — the gate tightens towards gold")
        fig.add_hline(y=0.999, line_dash="dot", annotation_text="gold accuracy SLA 99.9%")
        fig.add_hline(y=0.995, line_dash="dot", annotation_text="bronze completeness SLA 99.5%")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          yaxis_tickformat=".1%")
        st.plotly_chart(fig, width="stretch")

        st.subheader("Daily volume heatmap — event type × date")
        piv = (f_daily.groupby(["date", "event_type"], as_index=False)["display_volume"].sum()
               .pivot(index="event_type", columns="date", values="display_volume").fillna(0))
        fig_h = px.imshow(piv, aspect="auto", color_continuous_scale="Viridis",
                          labels={"color": "Events", "x": "Date", "y": "Event type"},
                          template="plotly_dark")
        fig_h.update_xaxes(tickformat="%b %d")
        fig_h.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_h, width="stretch")

        left, right = st.columns(2)
        with left:
            st.write("**Fault mix in dead_letter** (generator.corrupt kinds)")
            fault = pd.DataFrame({
                "fault_kind": FAULT_KINDS,
                "rejected": [int(f_daily["rejected"].sum() * w) for w in (0.34, 0.18, 0.31, 0.17)],
            })
            fig_f = px.bar(fault, x="fault_kind", y="rejected", color="fault_kind",
                           template="plotly_dark", labels={"rejected": "Rejected events"})
            fig_f.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig_f, width="stretch")
        with right:
            st.write("**SLA board**")
            st.dataframe(pd.DataFrame([
                ["fact_events (bronze)", "≤ 15 min", "≥ 99.5%", "≥ 99.9%", "🟢 holding"],
                ["gold.daily_active_users", "≤ 2 h", "100% surfaces", "±0.1%", "🟢 holding"],
                ["gold.retention_cohorts", "≤ 6 h", "all cohorts", "—", "🟡 watch 11-03"],
            ], columns=["Dataset", "Freshness", "Completeness", "Accuracy", "Status"]),
                width="stretch", hide_index=True)

# ----------------------------------------------------------------------------
with tab_anim:
    st.subheader("Press play — event volume evolving day by day")
    if f_daily.empty:
        st.info("No rows in the current filter.")
    else:
        anim = (f_daily.groupby(["date", "event_type"], as_index=False)["display_volume"].sum()
                .assign(frame=lambda d: d["date"].dt.strftime("%Y-%m-%d")))
        lim = float(anim["display_volume"].max()) or 1.0
        fig_a = px.bar(anim.sort_values(["date", "event_type"]), x="event_type", y="display_volume",
                       color="event_type", animation_frame="frame", range_y=[0, lim * 1.12],
                       template="plotly_dark", title="Daily volume by contract — animated (watch like collapse, Nov 2–4)",
                       labels={"display_volume": "Events", "event_type": "Event type"})
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 420
        st.plotly_chart(fig_a, width="stretch")

        cum = (anim.groupby(["date", "event_type"], as_index=False)["display_volume"].sum()
               .assign(cum=lambda d: d.groupby("event_type")["display_volume"].cumsum(),
                       frame=lambda d: d["date"].dt.strftime("%Y-%m-%d")))
        fig_c = px.line(cum.sort_values(["date", "event_type"]), x="date", y="cum", color="event_type",
                        animation_frame="frame", template="plotly_dark",
                        title="Cumulative volume race — animated",
                        labels={"cum": "Cumulative events", "date": "Date"})
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_c.layout.updatemenus:
            fig_c.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 420
        st.plotly_chart(fig_c, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

# ----------------------------------------------------------------------------
with tab_3d:
    st.subheader("🧊 3D quality space — drag to rotate, scroll to zoom")
    if f_daily.empty:
        st.info("No rows in the current filter.")
    else:
        d3 = f_daily.copy()
        d3["reject_pct"] = d3["reject_rate"] * 100
        d3["null_pct"] = d3["null_rate"] * 100
        d3["size"] = np.clip(np.sqrt(d3["display_volume"]) / 8, 3, 22)
        fig3 = px.scatter_3d(
            d3.sort_values("date"), x="date", y="null_pct", z="reject_pct",
            color="event_type", size="size", size_max=20, opacity=0.88,
            hover_data=["surface", "volume", "duplicate_count", "drift_count"],
            labels={"null_pct": "Null rate %", "reject_pct": "Reject rate %", "date": "Date"},
            template="plotly_dark",
            title="Every event × surface slice in date × null-rate × reject-rate space — outliers are incidents",
        )
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                           scene=dict(xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      zaxis=dict(backgroundcolor="rgba(20,24,32,.9)")),
                           margin=dict(l=0, r=0, t=44, b=0))
        st.plotly_chart(fig3, width="stretch")

        st.subheader("Reject-rate surface — event type × date terrain")
        surf = (f_daily.pivot_table(index="event_type", columns="date", values="reject_rate", aggfunc="mean")
                .fillna(0).sort_index())
        if surf.shape[1] > 2:
            z = (surf * 100).to_numpy()
            fig_s = go.Figure(data=[go.Surface(
                z=z, x=[d.strftime("%Y-%m-%d") for d in surf.columns], y=list(surf.index),
                colorscale="RdYlGn_r", colorbar=dict(title="Reject %"),
                cmin=0, cmax=float(max(np.nanmax(z), 1.0)),
            )])
            fig_s.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                scene=dict(xaxis_title="Date", yaxis_title="Event type",
                                           zaxis_title="Reject %",
                                           camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9))),
                                margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_s, width="stretch")
        else:
            st.info("Not enough date coverage in the current filter for a surface.")

# ----------------------------------------------------------------------------
with tab_contract:
    st.subheader("Versioned contracts — enforced at producer and at ingestion")
    st.dataframe(contracts, width="stretch", hide_index=True)
    st.caption("Source: `contracts/registry/*.v1.json` — `additionalProperties: false` is what turns "
               "an extra field into schema drift caught at the boundary (SchemaDriftDetector).")
    if not f_daily.empty:
        health = (f_daily.groupby("event_type", as_index=False)
                  .agg(volume=("volume", "sum"), attempted=("attempted", "sum"),
                       null_rate=("null_rate", "mean"), duplicates=("duplicate_count", "sum"),
                       drift=("drift_count", "sum")))
        health["pass_rate"] = health["volume"] / health["attempted"].clip(lower=1)
        fig_h2 = px.bar(health.sort_values("pass_rate"), x="pass_rate", y="event_type",
                        orientation="h", color="pass_rate", color_continuous_scale="RdYlGn",
                        range_x=[0.90, 1.0], template="plotly_dark",
                        labels={"pass_rate": "Contract pass rate", "event_type": "Event type"},
                        title="Contract pass rate by event type (filtered window)")
        fig_h2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             xaxis_tickformat=".1%")
        st.plotly_chart(fig_h2, width="stretch")
        st.dataframe(health, width="stretch", hide_index=True)

        st.subheader("Null-rate monitor — MONITORED_COLUMNS: event_id · user_id · surface · occurred_at")
        fig_n = px.line(f_daily.groupby(["date", "event_type"], as_index=False)["null_rate"].mean()
                        .sort_values("date"),
                        x="date", y="null_rate", color="event_type", markers=True,
                        template="plotly_dark",
                        labels={"null_rate": "Null rate", "date": "Date"},
                        title="Null rate by event type — the Nov 3 message_send spike is the critical triage record")
        fig_n.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            yaxis_tickformat=".1%")
        st.plotly_chart(fig_n, width="stretch")

# ----------------------------------------------------------------------------
with tab_triage:
    st.subheader("Triage queue — structured records with severity and suggested owner")
    st.caption("Shape mirrors `quality.triage.TriageRecord`: fingerprint · detector · signal · "
               "severity · event_type · detail · suggested_owner · evidence · first_seen. "
               "Re-running the engine upserts on fingerprint instead of spamming duplicates.")
    if f_triage.empty:
        st.success("No open triage records in the current filter — the gate is clean here.")
    else:
        sev_color = {"critical": "🔴", "warning": "🟠", "info": "🔵"}
        show = f_triage.copy()
        show["sev"] = show["severity"].map(sev_color)
        st.dataframe(
            show[["sev", "severity", "detector", "signal", "event_type", "detail",
                  "suggested_owner", "first_seen", "seen_count", "fingerprint"]],
            width="stretch", hide_index=True)
        a, b = st.columns(2)
        with a:
            fig_o = px.bar(f_triage.groupby(["suggested_owner", "severity"], as_index=False).size()
                           .rename(columns={"size": "records"}),
                           x="suggested_owner", y="records", color="severity",
                           color_discrete_map={"critical": "#ff4b4b", "warning": "#ffa421", "info": "#6ea8fe"},
                           template="plotly_dark", title="Open records by owner")
            fig_o.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_o, width="stretch")
        with b:
            fig_d = px.pie(f_triage, names="detector", template="plotly_dark",
                           title="Open records by detector")
            fig_d.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_d, width="stretch")
        st.markdown("**Owner map** (`quality/triage.py`): " + " · ".join(
            f"`{k}` → `{v}`" for k, v in OWNER_MAP.items()))
