from datetime import datetime as DT
import pyspark.sql.functions as F
from mozetl.clientsdaily.rollup import extract_search_counts, to_profile_day_aggregates
from mozetl.experimentsdaily.rollup import to_experiment_profile_day_aggregates

DEFAULT_VERSION = DT.strftime(DT.utcnow(), "%Y%m%d")

def save(data, base_path, name, output_version):
	output_path = "{}/{}/v{}/".format(base_path, name, output_version)
	data.coalesce(10).write.mode("overwrite").parquet(output_path)

# First, get the main summary data:
main_summary_path = "s3://telemetry-parquet/main_summary/v4/"
main_summary = spark.read.option("mergeSchema", "true").parquet(main_summary_path)

do_sampling = True
if do_sampling:
	main_summary = main_summary.where("sample_id = '42'")

# Next, get the data for experiments:
experiment_start = '20170907'
lookback_start = '20170810'  # 4 weeks earlier
experiment_id = '@unified-urlbar-shield-study-opt-out'

exp_branch_col_expr = "experiments['{}']".format(experiment_id)

experiments = main_summary.where(main_summary.submission_date_s3 >= experiment_start) \
                          .withColumn("experiment_branch", F.expr(exp_branch_col_expr)) \
                          .where("experiment_branch IS NOT NULL") \
                          .withColumn("experiment_id", F.lit(experiment_id)) \
                          .dropDuplicates("document_id")

# Stash the resulting dataframe:
output_bucket = "net-mozaws-prod-us-west-2-pipeline-analysis"
output_prefix = "mreid/unified_search_data"
output_base_path = "s3://{}/{}/".format(output_bucket, output_prefix)
output_version = DT.strftime(DT.utcnow(), "%Y%m%d")
exp_name = "main_summary_experiments"
save(experiments, output_base_path, exp_name, output_version)

# Then get all the client_ids in the study:
experiment_subjects = experiments.select("client_id").distinct()

# Finally, get the "look back" data for the same set of client_ids:
lookback_all = main_summary.where(main_summary.submission_date_s3 >= lookback_start) \
                           .withColumn("experiment_branch", F.expr(exp_branch_col_expr))
                           .where("experiment_branch IS NULL")
lookback = lookback_all.join(experiment_subjects, 'client_id', 'inner') \
                       .dropDuplicates("document_id")

lb_name = "lookback"
save(lookback, output_base_path, lb_name, output_version)

# Transform `experiments` into clients-daily form
exp_with_search = extract_search_counts(experiments)
exp_daily = to_experiment_profile_day_aggregates(exp_with_search)
exp_daily_name = "{}_daily".format(exp_name)
save(exp_daily, output_base_path, exp_daily_name, output_version)

# Transform `lookback` to clients-daily form:
from mozetl.clientsdaily.fields import ACTIVITY_DATE_COLUMN
lookback_with_day = lookback.select("*", ACTIVITY_DATE_COLUMN)
lookback_with_search = extract_search_counts(lookback_with_day)
lookback_daily = to_profile_day_aggregates(lookback_with_search)
lb_daily_name = "{}_daily".format(lb_name)
save(lookback_daily, output_base_path, lb_daily_name, output_version)


def extract():
	pass

def transform():
	pass

def load():
	pass

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

def main(input_bucket, input_prefix, output_bucket, output_prefix, output_version):
    """
    Extract unified search data, orient it like clients-daily.
    """
    spark = SparkSession.builder.appName("experiment_lookback_daily").getOrCreate()

    parquet_path = format_spark_path(input_bucket, input_prefix)
    frame = load_experiments_summary(spark, parquet_path)
    searches_frame = extract_search_counts(frame)
    results = to_experiment_profile_day_aggregates(searches_frame)
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )  # Don't write _SUCCESS files, which interfere w/ReDash discovery
    output_base_path = "{}/v{}".format(
        format_spark_path(output_bucket, output_prefix),
        output_version)
    results.write.mode("overwrite").parquet(output_base_path)


if __name__ == '__main__':
    main()
