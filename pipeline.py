"""Pipeline data projet jour 4 — MovieLens ml-latest-small.

Architecture : brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement depuis la racine du projet :
    python starter-code/pipeline.py
"""
from __future__ import annotations

import sys
import time

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
MOVIES_CSV    = "ml-latest-small/movies.csv"
SORTIE_SILVER = "output/silver/ratings"
SORTIE_GOLD   = "output/gold"
RATING_MIN    = 0.5
RATING_MAX    = 5.0
COL_PARTITION = "annee_note"
SEUIL_VOTES   = 50
TOP_N         = 3

# Schémas explicites (jamais inferSchema)
SCHEMA_RATINGS = StructType([
    StructField("userId",    IntegerType(), nullable=False),
    StructField("movieId",   IntegerType(), nullable=False),
    StructField("rating",    DoubleType(),  nullable=False),
    StructField("timestamp", LongType(),    nullable=False),
])

SCHEMA_MOVIES = StructType([
    StructField("movieId", IntegerType(), nullable=False),
    StructField("title",   StringType(),  nullable=True),
    StructField("genres",  StringType(),  nullable=True),
])


# ---------------------------------------------------------------------------
# Utilitaire chrono (comme TP08)
# ---------------------------------------------------------------------------
def chrono(label: str, fn) -> None:
    debut = time.perf_counter()
    fn()
    duree = time.perf_counter() - debut
    print(f"[chrono] {label} : {duree:.3f}s")


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
    df = df.withColumn("date_note",   F.to_date(F.from_unixtime(F.col("timestamp"))))
    df = df.withColumn(COL_PARTITION, F.year(F.col("date_note")))

    count_brut = df.count()
    print(f"Avant nettoyage          : {count_brut:>7} lignes")

    df = df.na.drop(subset=["userId", "movieId", "rating"])
    count_apres_na = df.count()
    print(f"Après na.drop            : {count_apres_na:>7} lignes  (écartées : {count_brut - count_apres_na})")

    df = df.dropDuplicates(subset=["userId", "movieId"])
    count_apres_dedup = df.count()
    print(f"Après dropDuplicates     : {count_apres_dedup:>7} lignes  (écartées : {count_apres_na - count_apres_dedup})")

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
# Étape 2a — chargement silver (avec cache, réutilisée par 3 analyses)
# ---------------------------------------------------------------------------
def charger_silver(spark) -> DataFrame:
    df = spark.read.parquet(SORTIE_SILVER)
    df = df.cache()
    df.count()  # matérialise le cache
    return df


# ---------------------------------------------------------------------------
# Étape 2b — chargement movies
# ---------------------------------------------------------------------------
def charger_movies(spark) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .schema(SCHEMA_MOVIES)
        .csv(MOVIES_CSV)
    )
    return df


# ---------------------------------------------------------------------------
# Contrôle d'intégrité référentielle — orphelins
# ---------------------------------------------------------------------------
def controle_orphelins(ratings: DataFrame, movies: DataFrame) -> DataFrame:
    """Détecte les ratings dont le movieId n'existe pas dans movies (left_anti join)."""
    orphelins = ratings.join(F.broadcast(movies), "movieId", "left_anti")
    nb_orphelins = orphelins.count()
    print(f"Contrôle orphelins : {nb_orphelins} note(s) sans film correspondant.")

    if nb_orphelins > 0:
        orphelins.select("movieId").distinct().show()
        return ratings.join(F.broadcast(movies.select("movieId")), "movieId", "left_semi")

    return ratings


# ---------------------------------------------------------------------------
# Analyse 1 — agrégation : films les mieux notés (avec seuil de votes)
# ---------------------------------------------------------------------------
def analyse_films_mieux_notes(ratings: DataFrame) -> DataFrame:
    df = (
        ratings
        .groupBy("movieId")
        .agg(
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
            F.count("*").alias("nb_votes"),
        )
        .filter(F.col("nb_votes") >= SEUIL_VOTES)
        .orderBy(F.desc("note_moyenne"))
    )
    df.show(10)
    return df


# ---------------------------------------------------------------------------
# Analyse 2 — jointure + broadcast : enrichissement titre et genres
# ---------------------------------------------------------------------------
def analyse_jointure_genres(films_agg: DataFrame, movies: DataFrame) -> DataFrame:
    df = (
        films_agg
        .join(F.broadcast(movies), "movieId", "inner")
        .select("movieId", "title", "genres", "note_moyenne", "nb_votes")
        .orderBy(F.desc("note_moyenne"))
    )
    df.show(10, truncate=False)
    return df


# ---------------------------------------------------------------------------
# Analyse 3 — window function : top TOP_N films par genre
# ---------------------------------------------------------------------------
def analyse_top_par_genre(films_enrichis: DataFrame) -> DataFrame:
    fenetre = Window.partitionBy("genre").orderBy(
        F.desc("note_moyenne"), F.desc("nb_votes")
    )
    df = (
        films_enrichis
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        .filter(~(F.col("genre") == "(no genres listed)"))
        .withColumn("rang", F.row_number().over(fenetre))
        .filter(F.col("rang") <= TOP_N)
        .select("genre", "rang", "title", "note_moyenne", "nb_votes")
        .orderBy("genre", "rang")
    )
    df.show(40, truncate=False)
    return df


# ---------------------------------------------------------------------------
# Optimisation mesurée — broadcast vs sort-merge join
# ---------------------------------------------------------------------------
def optimisation_broadcast(spark, films_agg: DataFrame, movies: DataFrame) -> None:
    print("\n=== Optimisation : broadcast vs sort-merge join ===")

    # a) sort-merge join (shuffle forcé)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    join_shuffle = films_agg.join(movies, "movieId")
    chrono("join sort-merge (shuffle)", lambda: join_shuffle.count())
    print("-- Plan sort-merge (doit montrer SortMergeJoin + Exchange) :")
    join_shuffle.explain()

    # b) broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # seuil toujours désactivé
    join_broadcast = films_agg.join(F.broadcast(movies), "movieId")
    chrono("join broadcast", lambda: join_broadcast.count())
    print("-- Plan broadcast (doit montrer BroadcastHashJoin, pas d'Exchange) :")
    join_broadcast.explain()

    # restaure le seuil par défaut (10 MiB)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

    print("Compare les plans : SortMergeJoin+Exchange vs BroadcastHashJoin.")


# ---------------------------------------------------------------------------
# Étape 2 — orchestration analyses (silver -> gold)
# ---------------------------------------------------------------------------
def transformation_et_analyses(spark) -> dict:
    ratings = charger_silver(spark)
    movies  = charger_movies(spark)

    ratings = controle_orphelins(ratings, movies)

    a1 = analyse_films_mieux_notes(ratings)
    a2 = analyse_jointure_genres(a1, movies)
    a3 = analyse_top_par_genre(a2)

    optimisation_broadcast(spark, a1, movies)

    return {
        "films_mieux_notes": a1,
        "films_avec_genres": a2,
        "top3_par_genre":    a3,
    }


# ---------------------------------------------------------------------------
# Étape 3 — écriture gold
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

    resultats = transformation_et_analyses(spark)
    ecrire_gold(resultats)

    input("Spark UI sur http://localhost:4040 — Entrée pour quitter...")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans pipeline.py.")
        sys.exit(1)
