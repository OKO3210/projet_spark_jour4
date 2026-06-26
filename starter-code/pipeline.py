"""Pipeline data projet jour 4 — MovieLens ml-latest-small.

Architecture : brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement depuis la racine du projet :
    python starter-code/pipeline.py
"""
from __future__ import annotations

import sys

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from spark_session import get_spark

# ---------------------------------------------------------------------------
# CONFIG — tous les chemins et seuils ici, aucun magic number ailleurs
# ---------------------------------------------------------------------------
RATINGS_CSV   = "ml-latest-small/ratings.csv"
MOVIES_CSV    = "ml-latest-small/movies.csv"   # étape 2
SORTIE_SILVER = "output/silver/ratings"
SORTIE_GOLD   = "output/gold"
RATING_MIN    = 0.5
RATING_MAX    = 5.0
COL_PARTITION = "annee_note"

# Schéma explicite ratings.csv (jamais inferSchema)
SCHEMA_RATINGS = StructType([
    StructField("userId",    IntegerType(), nullable=False),
    StructField("movieId",   IntegerType(), nullable=False),
    StructField("rating",    DoubleType(),  nullable=False),
    StructField("timestamp", LongType(),    nullable=False),
])


# ---------------------------------------------------------------------------
# Étape 1a — ingestion (bronze)
# ---------------------------------------------------------------------------
def ingestion(spark) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .schema(SCHEMA_RATINGS)
        .csv(RATINGS_CSV)
    )

    df.printSchema()
    print("Lignes brutes :", df.count())
    df.show(5)
    return df


# ---------------------------------------------------------------------------
# Étape 1b — nettoyage (bronze -> silver)
# ---------------------------------------------------------------------------
def nettoyage(df: DataFrame) -> DataFrame:
    # colonnes dérivées
    df = df.withColumn("date_note",  F.to_date(F.from_unixtime(F.col("timestamp"))))
    df = df.withColumn(COL_PARTITION, F.year(F.col("date_note")))

    count_brut = df.count()
    print(f"Avant nettoyage          : {count_brut:>7} lignes")

    # manquants sur colonnes critiques
    df = df.na.drop(subset=["userId", "movieId", "rating"])
    count_apres_na = df.count()
    print(f"Après na.drop            : {count_apres_na:>7} lignes  (écartées : {count_brut - count_apres_na})")

    # une note unique par (user, film)
    df = df.dropDuplicates(subset=["userId", "movieId"])
    count_apres_dedup = df.count()
    print(f"Après dropDuplicates     : {count_apres_dedup:>7} lignes  (écartées : {count_apres_na - count_apres_dedup})")

    # notes hors bornes
    df = df.filter(
        (F.col("rating") >= RATING_MIN) & (F.col("rating") <= RATING_MAX)
    )
    count_apres_filtre = df.count()
    print(f"Après filtre rating      : {count_apres_filtre:>7} lignes  (écartées : {count_apres_dedup - count_apres_filtre})")

    return df


# ---------------------------------------------------------------------------
# Étape 1c — écriture silver
# ---------------------------------------------------------------------------
def ecrire_silver(spark, df: DataFrame) -> None:
    df.write.mode("overwrite").partitionBy(COL_PARTITION).parquet(SORTIE_SILVER)
    print("Couche silver écrite dans", SORTIE_SILVER)

    # contrôle — relit le Parquet et vérifie la distribution par année (comme TP05)
    silver_df = spark.read.parquet(SORTIE_SILVER)
    silver_df.printSchema()
    (
        silver_df
        .groupBy(COL_PARTITION)
        .count()
        .orderBy(COL_PARTITION)
        .show()
    )


# ---------------------------------------------------------------------------
# Étape 2 — analyses (silver -> gold)  [TODO étape 2]
# ---------------------------------------------------------------------------
def transformation_et_analyses(spark) -> dict:
    df = spark.read.parquet(SORTIE_SILVER)

    df = df.cache()
    df.count()  # matérialise le cache

    # --- Analyse 1 : agrégation ------------------------------------------------
    analyse_1 = None  # TODO

    # --- Analyse 2 : jointure --------------------------------------------------
    analyse_2 = None  # TODO

    # --- Analyse 3 : window function ------------------------------------------
    analyse_3 = None  # TODO

    if analyse_1 is None or analyse_2 is None or analyse_3 is None:
        raise NotImplementedError(
            "TODO analyses : produisez 3 analyses (agrégation, jointure, window)."
        )

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


# ---------------------------------------------------------------------------
# Étape 3 — écriture gold  [TODO étape 2]
# ---------------------------------------------------------------------------
def ecrire_gold(resultats: dict) -> None:
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat écrit :", chemin)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    spark = get_spark("Projet Jour 4 - MovieLens")
    print("Spark UI disponible sur http://localhost:4040")

    brut   = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(spark, propre)

    # Décommenter pour étape 2 :
    # resultats = transformation_et_analyses(spark)
    # ecrire_gold(resultats)

    input("Spark UI sur http://localhost:4040 — Entrée pour quitter...")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
