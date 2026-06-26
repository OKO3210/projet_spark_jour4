"""Exploration AQE — mesure l'effet de shuffle.partitions et de l'AQE
sur une agrégation fixe lue depuis la couche silver.

Lancement depuis la racine du repo :
    python exploration_aqe.py
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession, functions as F

# ---------------------------------------------------------------------------
# CONFIG — aucun magic number ailleurs
# ---------------------------------------------------------------------------
SILVER             = "output/silver/ratings"
PARTITIONS_CHAUFFE = 64
LARGEUR_SEPARATEUR = 82

# (label, shuffle_partitions, aqe_active)
CONFIGS: list[tuple[str, int, bool]] = [
    ("200 partitions, AQE ON",  200, True),
    ("64 partitions, AQE ON",   64,  True),
    ("8 partitions, AQE ON",    8,   True),
    ("200 partitions, AQE OFF", 200, False),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def build_agg(silver: DataFrame) -> DataFrame:
    return silver.groupBy("movieId").agg(
        F.avg("rating").alias("note_moyenne"),
        F.count("*").alias("nb_votes"),
    )


def mesurer(spark: SparkSession, shuffle_partitions: int, aqe: bool) -> tuple[int, float]:
    """Applique les configs, exécute l'agrégation, retourne (partitions_réelles, durée_s)."""
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())

    # silver non cachée : on relit depuis Parquet pour mesurer le shuffle réel
    silver = spark.read.parquet(SILVER)
    agg = build_agg(silver)

    debut = time.perf_counter()
    agg.count()                          # déclenche le shuffle — AQE joue ici
    duree = time.perf_counter() - debut

    # Sans cache, reflète le nombre de partitions de sortie du plan ;
    # à ce volume AQE ne coalesce pas, correspond au shuffle.partitions demandé.
    partitions_reelles = agg.rdd.getNumPartitions()
    return partitions_reelles, duree


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    # Session créée à la main (pas get_spark) : on pilote shuffle.partitions et AQE
    # finement via spark.conf.set entre chaque mesure.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Exploration AQE")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Chauffe JVM — non chronométrée, évite le biais du premier job
    print("=== Chauffe JVM (non chronométrée) ===")
    spark.conf.set("spark.sql.shuffle.partitions", str(PARTITIONS_CHAUFFE))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    build_agg(spark.read.parquet(SILVER)).count()
    print("Chauffe terminée.\n")

    # Mesures
    resultats: list[tuple[str, int, bool, int, float]] = []

    for label, sp, aqe in CONFIGS:
        print(f"--- {label} ---")
        partitions_reelles, duree = mesurer(spark, sp, aqe)
        print(f"  partitions réelles : {partitions_reelles}   temps : {duree:.3f}s\n")
        resultats.append((label, sp, aqe, partitions_reelles, duree))

    # Tableau récapitulatif
    sep = "-" * LARGEUR_SEPARATEUR
    print("\n=== Tableau récapitulatif ===")
    print(sep)
    print(f"{'Label':<30} {'shuffle_part':>12} {'AQE':>5} {'part. réelles':>14} {'temps (s)':>10}")
    print(sep)
    for label, sp, aqe, pr, t in resultats:
        print(f"{label:<30} {sp:>12} {'ON' if aqe else 'OFF':>5} {pr:>14} {t:>10.3f}")
    print(sep)

    # explain() sur la dernière config (AQE OFF, 200) — doit montrer 200 partitions, pas de coalesce
    label_last, sp_last, aqe_last = CONFIGS[-1]
    print(f"\n=== explain() — '{label_last}' (AQE OFF : pas de coalesce attendu) ===")
    spark.conf.set("spark.sql.shuffle.partitions", str(sp_last))
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe_last).lower())
    agg_explain = build_agg(spark.read.parquet(SILVER))
    agg_explain.explain()

    spark.stop()


if __name__ == "__main__":
    main()
