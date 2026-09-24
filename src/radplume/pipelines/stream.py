"""Ścieżka B — STREAMING (czujniki, celowo brudne dane) + ścieżka C — CDC (rejestr urządzeń).

Przepływ (plan 2.2):
    rejestr CDC → silver.devices (SCD2)
    symulator → landing/sensor_stream → bronze.sensor_raw (append-only)
        → lag > 15 min → ops.sensor_late_rejected
        → dedup (watermark) → reguły twarde → silver.sensor_clean / ops.sensor_quarantine
    silver.sensor_clean → silver.sensor_quality (dryf, frozen, sąsiedzi)
        → gold.live_alerts, gold.sensor_dq_summary

Lokalnie strumień czyta katalog plików JSON; na Databricks ten sam kod
czytałby volume przez Auto Loader (``format("cloudFiles")``) — zmienia się
źródło, nie logika czyszczenia (plan 4.4a).

Trigger ``availableNow``: przetwórz wszystko, co czeka, i zakończ. Ten sam
kod działa jak ciągły strumień po zmianie triggera, ale w codziennej pracy
i w CI nie trzyma klastra włączonego (plan Część V, zasada oszczędzania nr 1).
"""

from __future__ import annotations

import datetime as dt
import logging
import random
import shutil
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from radplume.core.dq_metrics import log_metrics
from radplume.gold.live_alerts import build_dq_summary, build_live_alerts
from radplume.pipelines.context import Context
from radplume.silver.devices_cdc import apply_scd2, read_cdc
from radplume.silver.sensor_clean import (
    SENSOR_SCHEMA,
    apply_hard_rules,
    dedup,
    hard_rules,
    join_device_version,
    split_late,
)
from radplume.silver.sensor_quality import build_sensor_quality
from radplume.simulators.device_registry import cdc_events, plan_devices, write_cdc
from radplume.simulators.sensor_sim import DemoSchedule, SensorSimulator

log = logging.getLogger(__name__)


def _demo_start(ctx: Context) -> tuple[dict, dt.datetime]:
    demo = ctx.cfg["demo"]
    ep = (
        ctx.storage.read("silver", "scenarios")
        .where(
            (F.col("scenario_set") == demo["scenario_set"])
            & (F.col("site_id") == demo["site_id"])
            & (F.col("episode_id") == demo["episode_id"])
        )
        .select("episode_start")
        .first()
    )
    if ep is None:
        raise RuntimeError("Brak epizodu demo w silver.scenarios — uruchom najpierw ścieżkę batch")
    start = ep["episode_start"] + dt.timedelta(hours=demo["sim_start_offset_h"])
    return demo, start


# ----------------------------------------------------------------------------- CDC
def step_device_registry(ctx: Context) -> None:
    """Rozmieszcza czujniki, zapisuje feed CDC i buduje silver.devices (SCD2)."""
    st, cfg = ctx.storage, ctx.cfg
    demo, start = _demo_start(ctx)
    site = {**cfg["sites"][demo["site_id"]], "site_id": demo["site_id"]}

    # Najwyższa oczekiwana dawka w oknie symulacji (kilka godzin) — małe dane, collect() jest OK.
    window_end = start + dt.timedelta(minutes=cfg["sensor_sim"]["minutes"])
    dose = (
        st.read("gold", "expected_dose")
        .where((F.col("site_id") == demo["site_id"]) & F.col("hour_ts").between(start, window_end))
        .groupBy("cell_id").agg(F.max("model_dose_usv_h").alias("max_dose"))
    )
    grid = st.read("silver", "grid").where(F.col("site_id") == demo["site_id"])
    cells = dose.join(grid, "cell_id").where(F.col("dist_km") >= 3)
    plume = [r.asDict() for r in cells.orderBy(F.desc("max_dose"), "cell_id").limit(demo["n_plume_devices"] - 2).collect()]
    if not plume or plume[0]["max_dose"] <= 0:
        raise RuntimeError("Smuga nie dociera do żadnej komórki w oknie symulacji — zmień demo.sim_start_offset_h")
    bg_candidates = [
        r.asDict() for r in cells.where((F.col("max_dose") < 1e-3) & F.col("dist_km").between(10, 40))
        .orderBy("cell_id").collect()
    ]
    rng = random.Random(cfg["run"]["seed"])
    background = rng.sample(bg_candidates, demo["n_background_devices"])

    devices = plan_devices(plume, background, site, cfg["run"]["seed"])
    write_cdc(cdc_events(devices, start), st.landing())

    bronze = read_cdc(ctx.spark, st.landing("device_cdc"))
    st.overwrite(bronze, "bronze", "device_cdc")
    st.overwrite(apply_scd2(st.read("bronze", "device_cdc")), "silver", "devices")


# ----------------------------------------------------------------------------- symulator
def step_sensor_sim(ctx: Context) -> None:
    """Generuje pliki z odczytami czujników (scenariusz demo + losowe anomalie)."""
    st, cfg = ctx.storage, ctx.cfg
    demo, start = _demo_start(ctx)
    versions = [r.asDict() for r in st.read("silver", "devices").orderBy("device_id", "valid_from").collect()]

    devices, fw_changes, retired = {}, {}, {}
    for v in versions:
        devices.setdefault(v["device_id"], v)  # pierwsza wersja = pozycja i komórka
        fw_changes.setdefault(v["device_id"], []).append((v["valid_from"], v["firmware"]))
    for dev_id in devices:
        last = [v for v in versions if v["device_id"] == dev_id][-1]
        if last["valid_to"] is not None:
            retired[dev_id] = last["valid_to"]  # zamknięta ostatnia wersja = DELETE
    ordered = sorted(devices.values(), key=lambda d: d["device_id"])

    cells = [d["cell_id"] for d in ordered]
    dose_rows = st.read("gold", "expected_dose").where(F.col("cell_id").isin(cells)).collect()
    expected_dose = {(r["cell_id"], r["hour_ts"]): r["model_dose_usv_h"] for r in dose_rows}
    wind_rows = (
        st.read("silver", "meteo").where(F.col("site_id") == demo["site_id"]).select("time_utc", "wind_speed_ms").collect()
    )
    expected_wind = {r["time_utc"]: r["wind_speed_ms"] for r in wind_rows}

    sim = SensorSimulator(
        ordered, fw_changes, retired, expected_dose, expected_wind,
        cfg["sensor_dq"], cfg["physics"]["background_usv_h"], cfg["run"]["seed"], DemoSchedule(),
    )
    n = sim.run(start, cfg["sensor_sim"]["minutes"], st.landing("sensor_stream"), cfg["sensor_sim"]["realtime_sleep_s"])
    log.info("sensor_sim: wygenerowano %d odczytów od %s", n, start)


# ----------------------------------------------------------------------------- strumień
def _run_available_now(query_builder, name: str):
    q = query_builder.trigger(availableNow=True).queryName(name).start()
    q.awaitTermination()
    if q.exception():
        raise q.exception()
    return q


def step_sensor_stream(ctx: Context) -> None:
    """landing → bronze.sensor_raw → (late | clean | quarantine) — Structured Streaming."""
    st, cfg = ctx.storage, ctx.cfg
    dq = cfg["sensor_dq"]

    # 1) landing → bronze (append-only, niezmienny). Jawny schemat — nie inferujemy na strumieniu.
    raw = (
        ctx.spark.readStream.schema(SENSOR_SCHEMA)
        # Mniej plików na mikro-batch = więcej mikro-batchy. Celowo: lokalnie widać wtedy
        # działanie watermarku między batchami, jak w prawdziwym strumieniu.
        .option("maxFilesPerTrigger", 20)
        .json(st.landing("sensor_stream"))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    _bronze_sink(ctx, raw)

    # 2) bronze → spóźnione (bezstanowo, deterministycznie)
    stream = st.read_stream("bronze", "sensor_raw")
    _, late = split_late(stream, dq["late_arrival"]["max_lag_minutes"])
    _sink_append(ctx, late, "ops", "sensor_late_rejected", "sensor_late")

    # 3) bronze → na czas → dedup → foreachBatch: wersja urządzenia (SCD2) + reguły twarde
    on_time, _ = split_late(st.read_stream("bronze", "sensor_raw"), dq["late_arrival"]["max_lag_minutes"])
    deduped = dedup(on_time, dq["dedup"]["watermark_minutes"], dq["dedup"]["keys"])
    rules = hard_rules(dq)

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        # Tabela urządzeń jest mała i zmienia się rzadko — czytamy ją świeżo w każdym batchu.
        devices = st.read("silver", "devices")
        enriched = join_device_version(batch, devices)
        clean, quarantine = apply_hard_rules(enriched, rules)
        # txnAppId/txnVersion: powtórzony po awarii batch nie zapisze się drugi raz (exactly-once).
        st.append_idempotent(clean, "silver", "sensor_clean", "sensor_clean", batch_id)
        st.append_idempotent(quarantine, "ops", "sensor_quarantine", "sensor_quarantine", batch_id)

    q = _run_available_now(
        deduped.writeStream.foreachBatch(process_batch).option("checkpointLocation", st.checkpoint("sensor_main")),
        "sensor_main",
    )
    dropped = sum(
        sum(op.get("numRowsDroppedByWatermark", 0) for op in (p.get("stateOperators") or []))
        for p in q.recentProgress
    )
    # Kontrola z planu P1: watermark = reguła biznesowa, więc powinien niczego nie gubić.
    log_metrics(st, "sensor_stream", {"rows_dropped_by_watermark": dropped})


def _bronze_sink(ctx: Context, raw: DataFrame) -> None:
    _sink_append(ctx, raw, "bronze", "sensor_raw", "sensor_bronze")


def _sink_append(ctx: Context, df: DataFrame, layer: str, name: str, checkpoint: str) -> None:
    st = ctx.storage
    w = df.writeStream.format("delta").outputMode("append").option("checkpointLocation", st.checkpoint(checkpoint))
    w = w.option("mergeSchema", "true")
    if st.mode == "path":
        q = w.trigger(availableNow=True).queryName(checkpoint).start(st.table_path(layer, name))
    else:
        st._ensure_schema(layer)
        q = w.trigger(availableNow=True).queryName(checkpoint).toTable(st.table_name(layer, name))
    q.awaitTermination()
    if q.exception():
        raise q.exception()


# ----------------------------------------------------------------------------- jakość i alerty
def step_sensor_quality(ctx: Context) -> None:
    """Dryf (reszta vs model + sąsiedzi), zamrożenie, skok wiatru, warm-up."""
    st, cfg = ctx.storage, ctx.cfg
    q = build_sensor_quality(
        st.read("silver", "sensor_clean"), st.read("gold", "expected_dose"), cfg["sensor_dq"], cfg["physics"]["background_usv_h"]
    )
    st.overwrite(q, "silver", "sensor_quality")


def step_alerts(ctx: Context) -> None:
    """Alerty (sygnał vs usterka) i podsumowanie jakości danych dla dashboardu."""
    st, cfg = ctx.storage, ctx.cfg
    quality = st.read("silver", "sensor_quality")
    st.overwrite(build_live_alerts(quality, cfg["sensor_dq"], cfg["physics"]["background_usv_h"]), "gold", "live_alerts")
    quarantine = st.read("ops", "sensor_quarantine") if st.exists("ops", "sensor_quarantine") else quality.limit(0)
    late = st.read("ops", "sensor_late_rejected") if st.exists("ops", "sensor_late_rejected") else quality.limit(0)
    st.overwrite(build_dq_summary(st.read("bronze", "sensor_raw"), late, quarantine, quality), "gold", "sensor_dq_summary")


def step_sensor_reset(ctx: Context) -> None:
    """Czyści ścieżkę czujników (pliki, checkpointy, tabele) — do ponownego demo od zera."""
    st = ctx.storage
    if st.mode != "path":
        raise RuntimeError("sensor-reset działa tylko lokalnie; w chmurze usuń tabele świadomie (governance)")
    for p in [st.landing("sensor_stream"), st.landing("device_cdc"), st.checkpoint("sensor_bronze"),
              st.checkpoint("sensor_late"), st.checkpoint("sensor_main")]:
        shutil.rmtree(p, ignore_errors=True)
    for layer, name in [("bronze", "sensor_raw"), ("bronze", "device_cdc"), ("silver", "devices"),
                        ("silver", "sensor_clean"), ("silver", "sensor_quality"), ("ops", "sensor_late_rejected"),
                        ("ops", "sensor_quarantine"), ("gold", "live_alerts"), ("gold", "sensor_dq_summary")]:
        shutil.rmtree(Path(st.table_path(layer, name)), ignore_errors=True)
    log.info("sensor-reset: wyczyszczono ścieżkę czujników")


STREAM_STEPS = {
    "device-registry": step_device_registry,
    "sensor-sim": step_sensor_sim,
    "sensor-stream": step_sensor_stream,
    "sensor-quality": step_sensor_quality,
    "alerts": step_alerts,
    "sensor-reset": step_sensor_reset,
}
STREAM_ORDER = ["device-registry", "sensor-sim", "sensor-stream", "sensor-quality", "alerts"]
