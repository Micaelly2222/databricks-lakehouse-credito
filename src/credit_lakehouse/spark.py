from pyspark.sql import SparkSession


def get_spark(app_name: str = "credit-lakehouse") -> SparkSession:
    """No Databricks devolve a sessão já existente; fora dele cria uma sessão local com Delta Lake."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    try:
        from databricks.connect import DatabricksSession  # type: ignore

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        pass

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
