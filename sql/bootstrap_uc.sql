-- =============================================================================
-- One-off Unity Catalog bootstrap for smogcast (run once per workspace, BEFORE the first
-- `databricks bundle deploy`). Safe to re-run: every statement is IF NOT EXISTS.
--
-- Layout expected by packages/core/src/smogcast/core/conf/dev.yaml (and prod.yaml):
--   tables   smogcast_dev.{bronze,silver,gold,ops}.<table>
--   files    /Volumes/smogcast_dev/raw/landing       raw downloads (GIOŚ, Open-Meteo, RAG documents)
--            /Volumes/smogcast_dev/raw/checkpoints   Structured Streaming checkpoints
--            /Volumes/smogcast_dev/raw/models        saved Spark ML models
-- A volume path is /Volumes/<catalog>/<schema>/<volume>, so `raw` is a SCHEMA holding three volumes.
--
-- PROD: replace smogcast_dev with smogcast_prod (in stage E9 this runs from CI, not by hand).
-- =============================================================================

-- The metastore (metastore_azure_germanywestcentral) has NO default storage root, so a new catalog needs
-- a managed location. It lives under the workspace's existing external location `databricks_course_ws`
-- (checked 2026-10-06 with `databricks external-locations list`). PROD: its own workspace storage.
CREATE CATALOG IF NOT EXISTS smogcast_dev
  MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragekwe4l73eqcgmo.dfs.core.windows.net/7405617820175624/smogcast_dev'
  COMMENT 'smogcast: tomorrow''s PM10/PM2.5 exceedance forecast (DEV)';

CREATE SCHEMA IF NOT EXISTS smogcast_dev.bronze COMMENT 'raw data as delivered, long format';
CREATE SCHEMA IF NOT EXISTS smogcast_dev.silver COMMENT 'cleaned data, daily aggregates, model features';
CREATE SCHEMA IF NOT EXISTS smogcast_dev.gold   COMMENT 'forecast, backtest metrics, data-quality summary';
CREATE SCHEMA IF NOT EXISTS smogcast_dev.ops    COMMENT 'quarantine, late readings, data-quality metric log';
CREATE SCHEMA IF NOT EXISTS smogcast_dev.raw    COMMENT 'volumes with files: landing, checkpoints, models';

CREATE VOLUME IF NOT EXISTS smogcast_dev.raw.landing     COMMENT 'raw downloads; originals kept in _zrodlo/';
CREATE VOLUME IF NOT EXISTS smogcast_dev.raw.checkpoints COMMENT 'Structured Streaming checkpoints (pm-stream)';
CREATE VOLUME IF NOT EXISTS smogcast_dev.raw.models      COMMENT 'Spark ML pipeline models (train -> forecast)';
