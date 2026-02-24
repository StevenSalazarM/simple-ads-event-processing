import apache_beam as beam
import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import json
import traceback

# Import all the necessary transforms from transforms.py
from transforms import (
    DetectAndSplitDuplicatesFn, 
    FlattenJoinedDataFn, 
    AggregateMetricsFn, 
    WriteToJsonFn,
    AggregateAdvertiserMetricsFn,
    GetTopAdvertisersFn,
    AggregateUserSpendFn,
    CalculateMedianSpendFn
)

def run(options, impressions_path, clicks_path):
    clicks_data = None
    impressions_data = None

    try:
        with open(clicks_path, 'r') as f:
            clicks_data = json.load(f)

        with open(impressions_path, 'r') as f:
            impressions_data = json.load(f)
    except Exception as e:
        logging.error(f"Error reading JSON files: {e}")
        logging.error("Please check the file paths and ensure they are correct or that they fit into the primary memory (RAM).")
        logging.error(traceback.format_exc())
        return

    with beam.Pipeline(options=options) as pipeline:
            
            # Create Impressions PCollection, tag duplicate data and invalid data for further processing and tag clean data for the main pipeline
            impressions_split = (
                pipeline 
                | 'Create Impressions' >> beam.Create(impressions_data)
                | 'Map Imp ID' >> beam.Map(lambda x: (x['id'], x))
                | 'Group Imp by ID' >> beam.GroupByKey()
                | 'Split Imp Duplicates' >> beam.ParDo(DetectAndSplitDuplicatesFn()).with_outputs('duplicates', 'invalid', main='clean')
            )
            
            clean_impressions = impressions_split.clean
            impressions_dups = impressions_split.duplicates
            invalid_impressions = impressions_split.invalid

            # Create Clicks PCollection, tag duplicate data and invalid data for further processing and tag clean data for the main pipeline
            clicks_split = (
                pipeline 
                | 'Create Clicks' >> beam.Create(clicks_data)
                | 'Map Click ID' >> beam.Map(lambda x: (x['id'], x))
                | 'Group Clicks by ID' >> beam.GroupByKey()
                | 'Split Click Duplicates' >> beam.ParDo(DetectAndSplitDuplicatesFn()).with_outputs('duplicates', 'invalid', main='clean')
            )
            
            clean_clicks = clicks_split.clean
            clicks_dups = clicks_split.duplicates
            invalid_clicks = clicks_split.invalid

            # Data has been deduplicated and is clean
            # Create the key for clicks and impressions to perform the Join on impression_id
            keyed_clean_impressions = clean_impressions | 'Key Clean Imp' >> beam.Map(lambda x: (x['id'], x))
            keyed_clean_clicks = clean_clicks | 'Key Clean Clicks' >> beam.Map(lambda x: (x['impression_id'], x))

            joined_data = (
                {'impressions': keyed_clean_impressions, 'clicks': keyed_clean_clicks}
                | 'CoGroupByKey' >> beam.CoGroupByKey()
            )

            # Flatten the data ONCE so it can be routed into the 3 different branches
            flat_enriched_data = (
                joined_data 
                | 'Flatten Joined Data' >> beam.ParDo(FlattenJoinedDataFn()).with_outputs('invalid_clicks', main='clean')
            )
            clean_flat_enriched_data = flat_enriched_data.clean
            invalid_clicks_from_join = flat_enriched_data.invalid_clicks

            # ========================================================
            # BRANCH 1: Calculate how applications perform by country (total impressions, clicks, and revenue) and save the results in a JSON file (Goal 1)
            # ========================================================
            (
                clean_flat_enriched_data
                | 'G1: Key Data' >> beam.Map(lambda x: ((x['app_id'], x['country_code']), x))
                | 'G1: Group' >> beam.GroupByKey()
                | 'G1: Aggregate' >> beam.ParDo(AggregateMetricsFn())
                | 'G1: To List' >> beam.combiners.ToList()
                | 'G1: Write JSON' >> beam.ParDo(WriteToJsonFn('output/output_metrics.json'))
            )

            # ========================================================
            # BRANCH 2: Top 5 Advertisers for each app/country (Goal 2)
            # ========================================================
            (
                clean_flat_enriched_data
                | 'G2: Key Data' >> beam.Map(lambda x: ((x['app_id'], x['country_code'], x['advertiser_id']), x))
                | 'G2: Group By Adv' >> beam.GroupByKey()
                | 'G2: Filter & Calc RPM' >> beam.ParDo(AggregateAdvertiserMetricsFn())
                | 'G2: Group By App/Country' >> beam.GroupByKey()
                | 'G2: Top 5' >> beam.ParDo(GetTopAdvertisersFn())
                | 'G2: To List' >> beam.combiners.ToList()
                | 'G2: Write JSON' >> beam.ParDo(WriteToJsonFn('output/top_advertisers.json'))
            )

            # ========================================================
            # BRANCH 3: Median Spend (Goal 3)
            # ========================================================
            (
                clean_flat_enriched_data
                | 'G3: Key Data' >> beam.Map(lambda x: ((x['country_code'], x['user_id']), x['revenue']))
                | 'G3: Group By User' >> beam.GroupByKey()
                | 'G3: Sum Spend' >> beam.ParDo(AggregateUserSpendFn())
                | 'G3: Group By Country' >> beam.GroupByKey()
                | 'G3: Calc Median' >> beam.ParDo(CalculateMedianSpendFn())
                | 'G3: To List' >> beam.combiners.ToList()
                | 'G3: Write JSON' >> beam.ParDo(WriteToJsonFn('output/median_spend.json'))
            )

            # ========================================================
            # BRANCH 4: SIDE PIPELINE: Save Duplicates
            # ========================================================

            (
                clicks_dups
                | 'Write Duplicate Clicks from File' >> beam.ParDo(WriteToJsonFn('dlq/duplicate_clicks.json'))
            )
            (
                impressions_dups
                | 'Write Duplicate Impressions from File' >> beam.ParDo(WriteToJsonFn('dlq/duplicate_impressions.json'))
            )
            # ========================================================
            # BRANCH 5: SIDE PIPELINE: Save Invalid Data
            # ========================================================
            
            # Write invalid data to separate files
            (
                invalid_clicks_from_join
                | 'Write Invalid Clicks from Join' >> beam.ParDo(WriteToJsonFn('dlq/invalid_clicks_missing_imp.json'))
            )
            (
                invalid_clicks
                | 'Write Invalid Clicks from File' >> beam.ParDo(WriteToJsonFn('dlq/invalid_clicks.json'))
            )
            (
                invalid_impressions
                | 'Write Invalid Impressions from File' >> beam.ParDo(WriteToJsonFn('dlq/invalid_impressions.json'))
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser(description="This apache beam job accept three parameters: json impressions path, json clicks path and run mode")
    parser.add_argument(
        "--impressions_json_path", type=str, default="impressions.json", help="Impressions Path", required=False
    )
    parser.add_argument(
        "--clicks_json_path", type=str, default="clicks.json", help="Clicks Path", required=False
    )

    args, pipeline_args = parser.parse_known_args()
    
    # Setting the pipeline options
    runner = "DirectRunner"
    pipeline_options = PipelineOptions(
        runner=runner,
    )

    logging.info(f"parsed args are {args}")
    run(pipeline_options, impressions_path=args.impressions_json_path, clicks_path=args.clicks_json_path)