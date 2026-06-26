"""Helper de création de SparkSession pour le projet du jour 4.

Importez `get_spark` dans votre pipeline plutôt que de recréer une session à la main :

    from spark_session import get_spark
    spark = get_spark("Mon pipeline")

Vous n'avez normalement pas besoin de modifier ce fichier.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "Projet Jour 4", shuffle_partitions: int = 64) -> SparkSession:
    """Retourne une SparkSession configurée pour le mode local.

    Paramètres
    ----------
    app_name : str
        Nom de l'application, visible dans la Spark UI (onglet en haut).
    shuffle_partitions : int
        Nombre de partitions créées après un shuffle (groupBy, join, window).
        Le défaut de Spark est 200, ce qui est trop pour un laptop : on
        descend à 64 pour éviter des tâches minuscules et trop d'overhead.
        À ajuster selon le volume de vos données.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Mode local : Spark utilise tous les coeurs disponibles de la machine.
        .master("local[*]")
        # Nombre de partitions de shuffle raisonnable pour un volume de laptop.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # AQE est activé par défaut en Spark 3+ et 4 ; on l'explicite pour mémoire.
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    # Réduire le bruit dans la console : on ne garde que les avertissements.
    spark.sparkContext.setLogLevel("WARN")
    return spark
