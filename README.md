##  Projet technique Spark/AWS

### Objectif

Déterminer si la qualité de l'air est liée à la fréquentation des routes en Île-de-France sur 2016-2017.

### Données fournies

Ces données sont situées dans le répertoire `./data` de ce dépôt git.

Nous disposons en entrée :

- De données concernant le trafic routier, issues de https://www.data.gouv.fr/fr/datasets/trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/ .
- D'un référentiel de codes postaux INSEE, issu de https://www.data.gouv.fr/fr/datasets/correspondance-entre-les-codes-postaux-et-codes-insee-des-communes-francaises/ .
- De données concernant la qualité de l'air en Île de France, issues de https://www.data.gouv.fr/fr/datasets/indices-qualite-de-lair-citeair-journaliers-par-polluant-sur-lile-de-france-les-departements-les-communes-franciliennes-et-les-arrondissements-parisiens/ .


### Questions

A l'aide de Spark :

- Filtrer les données de trafic routier pour ne garder que celles qui sont utiles.
- Même opération pour les données qui concernent la qualité de l'air.
- Pour les deux prochaines étapes, calculer les coefficients de corrélation entre les taux de particules PM10, le dioxyde d'azote et  et le monoxyde de carbone à l'aide de la méthode https://spark.apache.org/docs/2.2.0/ml-statistics.html#correlation :
- Produire ces données calculées par semaine, dans un fichier parquet situé dans le dossier "/results/weekly" situé sur amazon S3.
- Produire ces données calculées mensuellement dans une table "results.monthly" de la base MariaDB fournie.
- Le ou les jobs concernés doivent pouvoir tourner plusieurs fois de suite (mais pas simultanément) en cas d'ajustement des données initiales.

Le code source spark doit être livré dans le dépôt où est situé cet énoncé : https://gitlab.adneom.net/poc-amazon/spark-tech-upgrade
Ne pas hésiter à rajouter une clef SSH dans gitlab pour y accéder via `git@gitlab.adneom.net:poc-amazon/spark-tech-upgrade.git` .

### Informations de connection

#### AWS

- url : `https://115260554696.signin.aws.amazon.com/console`
- user : `TMP-tech-upgrade`
- pass console : `R_9kvkOw+y{Y`
- api token : `AKIARVVQQPXENU6X6HWM`
- api secret key : `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`

#### GITLAB

- url : `https://gitlab.adneom.net`
- user : `techupgrade`
- pass : `saeyoomeuju0Ohshu9chai7sae4AeG`

#### MariaDB

- endpoint : `database-tech-upgrade-1.cuiokcmhbdpc.eu-central-1.rds.amazonaws.com`
- user : `admin`
- pass : `o3Y8Wnu0SJIFOieWPmxB`

### Environnement d'exécution

Nous mettons à disposition un environnement Amazon AWS qui permet d'utiliser les services suivants.

#### Stockage sur S3

Accès au bucket `lomg-tech-upgrade` via https://s3.console.aws.amazon.com/s3/buckets/lomg-tech-upgrade/?region=eu-central-1

#### Cluster hadoop sur EMR

Accès : https://eu-central-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-central-1

##### Création du cluster

Pour lancer un cluster, cliquer sur un cluster existant puis le cloner.

##### Lancement d'un job spark

* Déposer le jar sur S3.
* Dans la commande suivante : 
  * Remplacer `j-XXXX` par l'identifiant du cluster créé.
  * Remplacer la classe contenant le `main` ainsi que l'emplacement du jar sur S3.

```
aws emr add-steps --region eu-central-1 --cluster-id j-XXXX --steps Type=Spark,Name="Hello World",ActionOnFailure=CONTINUE,"Args=[--class,HelloWorld,s3://lomg-tech-upgrade/jar/helloworld_2.11.jar]"
```

##### Connexion au cluster

Utiliser la clef située dans `aws/upgrade-technique.pem`

#### Ecriture dans S3 à partir d'un job spark dans EMR

Utiliser l'option `path` du `DataFrameWriter` de la façon suivante:

```scala
Seq(
  (9, "lorem"),
  (14, "ipsum"),
  (-28, "dolor")
).toDF("id", "word")
  .write
  .mode(SaveMode.Overwrite)
  .option("path", "s3://lomg-tech-upgrade/upgrade-data/upgrade")
  .saveAsTable("upgrade_data")
```

### Solution

#### Exécution 

##### - Compilation 
```
sbt compile
```

##### - tests 
```
sbt test
```

##### - Génération du jar 
```
sbt package
```

* Nom du jar : `spark_tech_2.12-0.1.0-SNAPSHOT.jar`
* Chemin du jar : `https://s3.console.aws.amazon.com/s3/object/lomg-tech-test/jar/spark_tech_2.12-0.1.0-SNAPSHOT.jar`
