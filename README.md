# Starter-code - Projet pipeline Spark (Jour 4)

Squelette de départ pour le projet du jour 4. Il vous fournit une SparkSession pré-configurée et un
pipeline à trous (sections TODO). Vous le complétez avec le jeu de données que vous avez choisi.

Ce n'est pas obligatoire de partir d'ici : vous pouvez écrire vos propres scripts. Mais c'est un
bon point de départ si vous voulez aller droit à l'essentiel.

## Contenu

- `requirements.txt` : la seule dépendance, PySpark.
- `spark_session.py` : un helper qui retourne une SparkSession configurée (appName, `local[*]`,
  nombre de partitions de shuffle raisonnable). À importer, ne pas modifier.
- `pipeline.py` : le squelette du pipeline, avec des sections TODO claires (ingestion, nettoyage,
  transformation, analyse, écriture). C'est ici que vous travaillez.

## 1. Prérequis

- Python 3.9 ou supérieur.
- Java 17 ou 21 installé (Spark 4 exige Java 17 minimum). Vérifier avec `java -version`.

## 2. Installation

Depuis la racine du projet (`Spark-hetic-slides/`), de préférence dans un environnement virtuel :

```bash
python -m venv .venv
source .venv/bin/activate        # sous Windows : .venv\Scripts\activate
pip install -r starter-code/requirements.txt
```

Vérifier que PySpark répond :

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

## 3. Télécharger les données

Le script de téléchargement récupère le fil rouge (taxi NYC, table des zones, un département DVF,
MovieLens small) dans `data/datasets/` :

```bash
bash data/download.sh
```

Les fichiers sont volumineux. Vérifier l'espace disque (au moins 5 Go conseillés) et ne pas les
committer dans Git. Pour les autres jeux (accidents ONISR, DVF d'un autre département, MovieLens
25M), voir les URLs dans `data/sources-open-data.md`.

## 4. Lancer le pipeline

Depuis la racine du projet :

```bash
python starter-code/pipeline.py
```

Tant que vous n'avez pas rempli les TODO, le pipeline s'arrête proprement avec un message vous
indiquant la prochaine étape à coder.

## 5. Ouvrir la Spark UI

Pendant qu'un job tourne, la Spark UI est disponible sur http://localhost:4040 (port 4041, 4042...
si plusieurs sessions tournent). C'est là que vous lisez les jobs, les stages, le DAG et le cache.

Si le pipeline se termine trop vite pour avoir le temps d'ouvrir l'UI, ajoutez un
`input("Entree pour quitter...")` avant le `spark.stop()` pour garder la session vivante.

## 6. Où trouver de l'aide

- L'énoncé complet et la grille d'évaluation : `projects/projet-jour-4.md`.
- Les exercices des jours 1 à 3 : dossier `exercises/`.
