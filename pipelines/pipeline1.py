import apache_beam as beam

def run_pipeline():
    with beam.Pipeline() as p:
        (
            p 
            | 'Create' >> beam.Create([1, 2, 3])
            | 'Print' >> beam.Map(print)
        )

run_pipeline()