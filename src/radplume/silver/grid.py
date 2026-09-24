"""silver.grid — siatka obliczeniowa wokół każdej lokalizacji + przypisanie miast do komórek.

Układ współrzędnych (plan 3.3 etap 3: „CRS zapisany w schemacie”):
lokalna płaszczyzna styczna w kilometrach, środek = źródło, oś x na wschód,
oś y na północ („LOCAL_EQUIRECT_KM”). Dlaczego nie UTM/pyproj/Sedona:
- w promieniu 100 km błąd przybliżenia równoprostokątnego jest < 0,5%,
  czyli dużo mniej niż niepewność samego modelu gaussowskiego,
- zostajemy przy czystych wyrażeniach Sparka — bez natywnych bibliotek,
  które trzeba by instalować na klastrze,
- mapowanie punkt → komórka to prosta arytmetyka (bez joinu przestrzennego),
  co skaluje się liniowo.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

KM_PER_DEG_LAT = 111.32
CRS_NAME = "LOCAL_EQUIRECT_KM"
EARTH_RADIUS_KM = 6371.0


def sites_df(spark: SparkSession, sites: dict[str, dict]) -> DataFrame:
    """Mała tabela lokalizacji (kilka wierszy) — bezpieczna do broadcast joinów."""
    rows = [
        (s["site_id"], s["name"], s["country_code"], s["jurisdiction_code"], float(s["lat"]), float(s["lon"]))
        for s in sites.values()
    ]
    return spark.createDataFrame(
        rows, "site_id STRING, site_name STRING, country_code STRING, jurisdiction_code STRING, site_lat DOUBLE, site_lon DOUBLE"
    )


def km_offsets(lat: Column, lon: Column, lat0: Column, lon0: Column) -> tuple[Column, Column]:
    """(x_km, y_km) punktu względem źródła (lat0, lon0)."""
    x = (lon - lon0) * F.lit(KM_PER_DEG_LAT) * F.cos(F.radians(lat0))
    y = (lat - lat0) * F.lit(KM_PER_DEG_LAT)
    return x, y


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Odległość po powierzchni Ziemi — do filtra „miasta w promieniu 100 km”."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def bearing_deg(x_km: Column, y_km: Column) -> Column:
    """Azymut od źródła do punktu (0° = północ, 90° = wschód)."""
    return F.pmod(F.degrees(F.atan2(x_km, y_km)), F.lit(360.0))


def cell_id(site_id: Column, ix: Column, iy: Column) -> Column:
    return F.concat_ws("_", site_id, ix.cast("string"), iy.cast("string"))


def build_grid(sites: DataFrame, grid_size: int, cell_km: float) -> DataFrame:
    """Komórki siatki ``grid_size × grid_size`` dla każdej lokalizacji.

    ``spark.range`` generuje indeksy po stronie klastra — żadnej pętli w Pythonie
    (przy siatce 101×101 × 4 lokalizacje to ~40 tys. wierszy, ale zasada ta sama
    przy każdej skali).
    """
    half = (grid_size - 1) / 2
    idx = sites.sparkSession.range(grid_size * grid_size).select(
        (F.col("id") % grid_size).cast("int").alias("ix"),
        (F.col("id") / grid_size).cast("int").alias("iy"),
    )
    g = sites.crossJoin(idx)  # lokalizacji jest kilka — iloczyn jest mały
    g = g.withColumn("x_km", (F.col("ix") - F.lit(half)) * F.lit(cell_km)).withColumn(
        "y_km", (F.col("iy") - F.lit(half)) * F.lit(cell_km)
    )
    return g.select(
        "site_id",
        cell_id(F.col("site_id"), F.col("ix"), F.col("iy")).alias("cell_id"),
        "ix",
        "iy",
        "x_km",
        "y_km",
        (F.col("site_lat") + F.col("y_km") / F.lit(KM_PER_DEG_LAT)).alias("lat"),
        (
            F.col("site_lon")
            + F.col("x_km") / (F.lit(KM_PER_DEG_LAT) * F.cos(F.radians("site_lat")))
        ).alias("lon"),
        F.sqrt(F.col("x_km") ** 2 + F.col("y_km") ** 2).alias("dist_km"),
        F.lit(CRS_NAME).alias("crs"),
        F.lit(cell_km).alias("cell_km"),
    )


def assign_points_to_cells(
    points: DataFrame, sites: DataFrame, grid_size: int, cell_km: float, radius_km: float
) -> DataFrame:
    """Przypisuje punkty (miasta, pomiary) do komórek siatki KAŻDEJ lokalizacji w zasięgu.

    Oczekuje kolumn ``lat``, ``lon`` w ``points``. Indeks komórki liczymy
    wzorem (zaokrąglenie przesunięcia / rozmiar komórki), więc nie potrzeba
    joinu z tabelą siatki ani indeksu przestrzennego.
    """
    half = (grid_size - 1) / 2
    j = points.crossJoin(F.broadcast(sites))  # lokalizacji jest kilka → broadcast
    x, y = km_offsets(F.col("lat"), F.col("lon"), F.col("site_lat"), F.col("site_lon"))
    j = (
        j.withColumn("distance_km", haversine_km(F.col("site_lat"), F.col("site_lon"), F.col("lat"), F.col("lon")))
        .where(F.col("distance_km") <= radius_km)
        .withColumn("x_km", x)
        .withColumn("y_km", y)
        .withColumn("ix", F.round(F.col("x_km") / cell_km + half).cast("int"))
        .withColumn("iy", F.round(F.col("y_km") / cell_km + half).cast("int"))
        .where(F.col("ix").between(0, grid_size - 1) & F.col("iy").between(0, grid_size - 1))
        .withColumn("cell_id", cell_id(F.col("site_id"), F.col("ix"), F.col("iy")))
        .withColumn("bearing_deg", bearing_deg(F.col("x_km"), F.col("y_km")))
    )
    return j.drop("site_lat", "site_lon")
