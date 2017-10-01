import logging
from datetime import timedelta, datetime as DT
import pyspark.sql.functions as F
from mozetl.utils import format_spark_path
from mozetl.clientsdaily.rollup import extract_search_counts, to_profile_day_aggregates
from mozetl.experimentsdaily.rollup import to_experiment_profile_day_aggregates

logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
DF = "%Y%m%d"
DEFAULT_VERSION = DT.strftime(DT.utcnow(), DF)


def save(data, base_path, name, output_version):
    output_path = "{}/{}/v{}/".format(base_path, name, output_version)
    logger.info("Saving {} to {}".format(name, output_path))
    data.coalesce(10).write.mode("overwrite").parquet(output_path)

def extract_main_summary(input_bucket, input_prefix, sample_id, experiment_end_date):
    main_summary_path = format_spark_path(input_bucket, input_prefix)
    logger.info("Reading main_summary data from {}".format(main_summary_path))
    main_summary = spark.read.option("mergeSchema", "true").parquet(main_summary_path)

    if sample_id:
        logger.info("Applying sample_id {}".format(sample_id))
        main_summary = main_summary.where("sample_id = '{}'".format(sample_id))

    if experiment_end_date:
        logger.info("Limiting to data on or before {}".format(experiment_end_date))
        main_summary = main_summary.where(main_summary.submission_date_s3 <= experiment_end_date)
    return main_summary


def get_branch_expr(experiment_id):
    return "experiments['{}']".format(experiment_id)


def extract_experiments(main_summary, experiment_id, start_date):
    logger.info("Gathering experiment data for '{}'".format(experiment_id))
    branch_expr = get_branch_expr(experiment_id)
    experiments = main_summary.where(main_summary.submission_date_s3 >= start_date) \
                              .withColumn("experiment_branch", F.expr(branch_expr)) \
                              .where("experiment_branch IS NOT NULL") \
                              .withColumn("experiment_id", F.lit(experiment_id)) \
                              .dropDuplicates(["document_id"])
    return experiments


def rewind_date(start_date_str, rewind_days, date_format=DF):
    date_parsed = DT.strptime(start_date_str, date_format)
    return DT.strftime(date_parsed - timedelta(rewind_days), date_format)

def get_lookback(main_summary, lookback_start, experiment_id, clients):
    logger.info("Fetching lookback data back to {}".format(lookback_start))
    branch_expr = get_branch_expr(experiment_id)
    lookback_all = main_summary.where(main_summary.submission_date_s3 >= lookback_start) \
                               .withColumn("experiment_branch", F.expr(branch_expr)) \
                               .where("experiment_branch IS NULL")

    logger.info("Joining with client_ids".format(name, output_path))
    lookback = lookback_all.join(clients, 'client_id', 'inner') \
                           .dropDuplicates(["document_id"])
    return lookback


@click.command()
@click.option('--input-bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input-prefix',
              default='main_summary/v4/',
              help='Prefix of the input dataset')
@click.option('--output-bucket',
              default='net-mozaws-prod-us-west-2-pipeline-analysis',
              help='Bucket of the output dataset')
@click.option('--output-prefix',
              default='/mreid/unified_search_data',
              help='Prefix of the output dataset')
@click.option('--output-version',
              default=DEFAULT_VERSION,
              help='Version of the output dataset')
@click.option('--experiment-id',
              default='@unified-urlbar-shield-study-opt-out',
              help='Target experiment')
@click.option('--experiment-start-date',
              default='20170907',
              help='Date the experiment was launched')
@click.option('--experiment-end-date',
              help='Optional date to end on')
@click.option('--lookback-days',
              default=28,
              help='How many days prior to the experiment to include data')
@click.option('--sample-id',
              default=None,
              help='Sample_id to restrict results to')

def main(input_bucket, input_prefix, output_bucket, output_prefix,
         output_version, experiment_id, experiment_start_date,
         experiment_end_date, lookback_days, sample_id):
    """
    Extract unified search data, orient it like clients-daily.
    """
    spark = SparkSession.builder.appName("experiment_lookback_daily").getOrCreate()

    # First, get the main summary data:
    main_summary = extract_main_summary(input_bucket, input_prefix, sample_id, experiment_end_date)

    # Next, get the data for experiments:
    experiments = extract_experiments(main_summary, experiment_id, experiment_start_date)

    # Stash the resulting dataframe:
    output_base_path = format_spark_path(output_bucket, output_prefix)
    exp_name = "experiments"
    save(experiments, output_base_path, exp_name, output_version)

    # Transform `experiments` into clients-daily form
    logger.info("Aggregating experiments by client-day")
    exp_with_search = extract_search_counts(experiments)
    exp_daily = to_experiment_profile_day_aggregates(exp_with_search)
    exp_daily_name = "{}_daily".format(exp_name)
    save(exp_daily, output_base_path, exp_daily_name, output_version)

    # Then get all the client_ids in the study:
    logger.info("Getting experimental client_ids")
    experiment_subjects = experiments.select("client_id").distinct()

    # Finally, get the "look back" data for the same set of client_ids:
    lookback_start = rewind_date(experiment_start_date, lookback_days)
    lookback = get_lookback(main_summary, lookback_start, experiment_subjects)
    lb_name = "lookback"
    save(lookback, output_base_path, lb_name, output_version)

    # Transform `lookback` to clients-daily form:
    logger.info("Aggregating lookback by client-day")
    from mozetl.clientsdaily.fields import ACTIVITY_DATE_COLUMN
    lookback_with_day = lookback.select("*", ACTIVITY_DATE_COLUMN)
    lookback_with_search = extract_search_counts(lookback_with_day)
    lookback_daily = to_profile_day_aggregates(lookback_with_search)
    lb_daily_name = "{}_daily".format(lb_name)
    save(lookback_daily, output_base_path, lb_daily_name, output_version)

    logger.info("All done for experiment '{}' for {}".format(experiment_id, output_version))


if __name__ == '__main__':
    main()
