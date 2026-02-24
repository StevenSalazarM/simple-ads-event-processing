import apache_beam as beam
import json

# --- 1. SHARED TRANSFORMS ---
class ValidateAndKeyFn(beam.DoFn):
    """
    Validates that a dictionary contains all required fields.
    If valid, yields a tuple of (key, element) for downstream grouping.
    If invalid, diverts the raw dictionary to a 'malformed' side output.
    """
    def __init__(self, required_fields, key_field):
        self.required_fields = required_fields
        self.key_field = key_field

    def process(self, element):
        # Check if ALL required fields are present and not None
        is_valid = all(field in element and element[field] is not None for field in self.required_fields)
        
        if is_valid:
            # Safe to extract the key
            yield (element[self.key_field], element)
        else:
            # Divert to Dead Letter Queue (DLQ)
            yield beam.pvalue.TaggedOutput('malformed', element)
            
class DetectAndSplitDuplicatesFn(beam.DoFn):
    def process(self, element):
        record_id, records_iterable = element
        records = list(records_iterable)
        
        try:
            if record_id is None or not isinstance(record_id, str)  or record_id.strip() == '':
                # yield invalid records
                yield beam.pvalue.TaggedOutput('invalid', {
                    'id': record_id,
                    'record': records,
                    'error': 'Invalid or missing ID'
                })
            else:
                # yield only the first record in case of duplicates
                yield records[0]
                
                if len(records) > 1:
                    # yield duplicate count
                    yield beam.pvalue.TaggedOutput('duplicates', {
                        'id': record_id,
                        'duplicate_count': len(records)
                    })
        except Exception as e:
            # In case of any unexpected error, we can log it or yield to an 'error' output for further analysis
            yield beam.pvalue.TaggedOutput('invalid', {
                'id': record_id,
                'record': records,
                'error': str(e)
            })

class FlattenJoinedDataFn(beam.DoFn):
    """Runs immediately after CoGroupByKey to clean and flatten the data ONCE."""
    def process(self, element):
        imp_id, grouped_data = element
        impressions = grouped_data['impressions']
        clicks = grouped_data['clicks']

        # if a click exists without an impression, we ignore it since it's invalid data. 
        # We only want to process valid impressions and their associated clicks.
        if not impressions or len(impressions) == 0:
            yield beam.pvalue.TaggedOutput('invalid_clicks', {
                'impression_id': imp_id,
                'clicks': clicks,
                'error': 'Click(s) without corresponding impression'
            })
        else:
            imp = impressions[0]
            
            # Calculate metrics once
            click_count = len(clicks)
            revenue = sum([c.get('revenue') or 0.0 for c in clicks])

            # Yield one rich, flat dictionary containing everything needed for all 3 goals
            yield {
                'app_id': imp.get('app_id'),
                'country_code': imp.get('country_code'),
                'advertiser_id': imp.get('advertiser_id'),
                'user_id': imp.get('user_id'),
                'impressions': 1,
                'clicks': click_count,
                'revenue': revenue
            }

class WriteToJsonFn(beam.DoFn):
    def __init__(self, output_filename):
        self.output_filename = output_filename

    def process(self, element):
        with open(self.output_filename, 'w') as f:
            json.dump(element, f, indent=2)
        print(f"Successfully wrote aggregated data to {self.output_filename}")
        yield element


# --- 2. GOAL 1: BASIC METRICS ---

class AggregateMetricsFn(beam.DoFn):
    def process(self, element):
        (app_id, country_code), metrics_list = element
        
        yield {
            'app_id': app_id,
            'country_code': country_code,
            'impressions': sum(m['impressions'] for m in metrics_list),
            'clicks': sum(m['clicks'] for m in metrics_list),
            'revenue': round(sum(m['revenue'] for m in metrics_list), 5)
        }


# --- 3. GOAL 2: TOP ADVERTISERS ---

class AggregateAdvertiserMetricsFn(beam.DoFn):
    def process(self, element):
        (app_id, country_code, advertiser_id), metrics = element
        
        total_impressions = sum(m['impressions'] for m in metrics)
        total_revenue = sum(m['revenue'] for m in metrics)
        
        if total_impressions >= 5:
            rpm = total_revenue / total_impressions
            yield ((app_id, country_code), {
                'advertiser_id': advertiser_id,
                'rpm': rpm
            })

class GetTopAdvertisersFn(beam.DoFn):
    def process(self, element):
        (app_id, country_code), advertisers = element
        sorted_advs = sorted(list(advertisers), key=lambda x: x['rpm'], reverse=True)
        
        yield {
            'app_id': app_id,
            'country_code': country_code,
            'recommended_advertiser_ids': [x['advertiser_id'] for x in sorted_advs[:5]]
        }


# --- 4. GOAL 3: MEDIAN SPEND ---

class AggregateUserSpendFn(beam.DoFn):
    def process(self, element):
        (country_code, user_id), revenues = element
        # Sum revenue for this user, yield keyed by country only
        yield (country_code, sum(revenues))

class CalculateMedianSpendFn(beam.DoFn):
    def process(self, element):
        country_code, user_spends = element
        spends_list = sorted(list(user_spends))
        
        if not spends_list:
            return
            
        n = len(spends_list)
        if n % 2 == 1:
            median = spends_list[n // 2]
        else:
            median = (spends_list[n // 2 - 1] + spends_list[n // 2]) / 2.0
            
        yield {
            'country_code': country_code,
            'median_spend': round(median, 5)
        }