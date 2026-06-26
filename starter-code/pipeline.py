"""Squelette de pipeline data pour le projet du jour 4.

Complétez les sections marquées TODO avec le jeu de données que vous avez choisi
(taxi NYC multi-mois, DVF immobilier, accidents ONISR, ou MovieLens).

Architecture cible (vue en cours) :
    brut (bronze) -> nettoyé (silver, Parquet) -> agrégé (gold, résultats)

Lancement, depuis la racine du projet :
    python starter-code/pipeline.py

L'énoncé complet et la grille : projects/projet-jour-4.md
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import get_spark

# Chemins. Adaptez-les au jeu de données que vous avez choisi.
# Exemple taxi multi-mois : "data/datasets/yellow_tripdata_2024-*.parquet"
DATA_BRUT = "data/datasets/yellow_tripdata_2024-01.parquet"
ZONES_CSV = "data/datasets/taxi_zone_lookup.csv"
SORTIE_SILVER = "data/output/clean"
SORTIE_GOLD = "data/output/analyses"


def ingestion(spark):
    """Étape 1a : lire les données brutes.

    TODO :
    - Lire vos données brutes (Parquet : spark.read.parquet ; CSV : spark.read.csv).
    - Pour du CSV, définir un SCHÉMA EXPLICITE (StructType) plutôt que inferSchema :
      plus sûr et plus rapide. Mettre option("sep", ";") pour les CSV français.
    - Inspecter : printSchema(), show(5), count().
    """
    df = spark.read.parquet(DATA_BRUT)

    df.printSchema()
    print("Lignes brutes :", df.count())
    return df


def nettoyage(df):
    """Étape 1b : typer, dériver des colonnes, nettoyer (bronze -> silver).

    TODO :
    - Créer vos colonnes dérivées avec withColumn (durée, prix au km/m2, heure...).
    - PROTÉGER les divisions : F.when(denominateur > 0, ...).otherwise(None).
    - Filtrer les valeurs aberrantes (montants négatifs, distances/surfaces nulles,
      dates incohérentes). Utiliser & | ~ (pas and/or/not) et parenthéser.
    - Retirer les doublons (dropDuplicates) et gérer les manquants (na.drop/na.fill).
    """
    # Exemple (taxi) à remplacer ou compléter :
    # df = df.withColumn(
    #     "duree_min",
    #     (F.col("tpep_dropoff_datetime").cast("long")
    #      - F.col("tpep_pickup_datetime").cast("long")) / 60,
    # )
    # df = df.filter((F.col("duree_min") > 0) & (F.col("duree_min") < 180))

    raise NotImplementedError(
        "TODO nettoyage : dérivez vos colonnes et filtrez les valeurs aberrantes."
    )


def ecrire_silver(df):
    """Étape 1c : écrire la couche intermédiaire nettoyée en Parquet.

    TODO :
    - Écrire en Parquet (write.mode("overwrite").parquet(SORTIE_SILVER)).
    - Optionnel : partitionBy sur une colonne à FAIBLE cardinalité (mois, département,
      année). Jamais sur une colonne à forte cardinalité (cela crée trop de fichiers).
    """
    df.write.mode("overwrite").parquet(SORTIE_SILVER)
    print("Couche silver écrite dans", SORTIE_SILVER)


def transformation_et_analyses(spark):
    """Étape 2 : relire le propre, puis 3 analyses (silver -> gold).

    On relit la couche Parquet nettoyée (pas les données brutes).

    TODO : produire AU MOINS TROIS analyses, dont :
    - une AGRÉGATION (groupBy + agg) ;
    - une JOINTURE (join, idéalement avec F.broadcast sur la petite table) ;
    - une WINDOW FUNCTION (Window.partitionBy(...).orderBy(...), row_number/rank/lag).
    Et au moins UNE OPTIMISATION justifiée : broadcast, cache, ou repartition.
    """
    df = spark.read.parquet(SORTIE_SILVER)

    # Optimisation cache : utile UNIQUEMENT si df est réutilisé par plusieurs analyses.
    df = df.cache()
    df.count()  # matérialise le cache

    # --- Analyse 1 : agrégation -------------------------------------------------
    # TODO : groupBy(...).agg(F.count, F.avg, F.sum...) sur une question métier.
    analyse_1 = None

    # --- Analyse 2 : jointure ---------------------------------------------------
    # TODO : charger une table de référence et la joindre.
    # Pensez à F.broadcast(petite_table) pour éviter un shuffle.
    analyse_2 = None

    # --- Analyse 3 : window function -------------------------------------------
    # TODO : classement / cumul / moyenne glissante par groupe.
    # fenetre = Window.partitionBy("groupe").orderBy(F.desc("metrique"))
    # ... .withColumn("rang", F.row_number().over(fenetre)).filter(F.col("rang") <= 10)
    analyse_3 = None

    if analyse_1 is None or analyse_2 is None or analyse_3 is None:
        raise NotImplementedError(
            "TODO analyses : produisez 3 analyses (agrégation, jointure, window)."
        )

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats de synthèse.

    TODO :
    - Écrire chaque résultat (Parquet ou CSV). coalesce(1) est acceptable ICI car les
      résultats agrégés sont PETITS. Ne jamais coalesce(1) un gros DataFrame.
    """
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Résultat écrit :", chemin)


def main():
    spark = get_spark("Projet Jour 4 - Mon pipeline")
    print("Spark UI disponible sur http://localhost:4040")

    # Étape 1 : ingestion et nettoyage (bronze -> silver)
    brut = ingestion(spark)
    propre = nettoyage(brut)
    ecrire_silver(propre)

    # Étape 2 : transformation et analyses (silver -> gold)
    resultats = transformation_et_analyses(spark)

    # Étape 3 : finalisation
    ecrire_gold(resultats)

    # Garder la session vivante pour explorer la Spark UI.
    # Décommentez la ligne suivante si le pipeline se termine trop vite :
    # input("Spark UI sur http://localhost:4040 - Entree pour quitter...")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print()
        print("Pipeline incomplet :", e)
        print("Complétez les sections TODO dans starter-code/pipeline.py.")
        sys.exit(1)
