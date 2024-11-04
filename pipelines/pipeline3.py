import apache_beam as beam
from apache_beam import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import sys

# Set logging level
logging.basicConfig(level=logging.INFO)

class LogElementDoFn(DoFn):
    def process(self, element, step_name):
        # Log completion message with the step name
        logging.info(f"{step_name} completed with element: {element}")
        yield element  # Pass the element downstream without modification

def run_pipeline():
    # Define pipeline options with the `flags=[]` argument to ignore Jupyter-specific args
    pipeline_options = PipelineOptions(flags=[], streaming=False)

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Create initial input data and log
        input_data = (
            p
            | 'Create Input' >> beam.Create(['Data inputted'])
            | 'Log Input Data' >> beam.ParDo(LogElementDoFn(), step_name='Input Data')
        )

        # Step 2: First transformation with logging
        transformed_subscriptions = (
            input_data
            | 'Subscriptions' >> beam.Map(lambda x: x + " -> Subscriptions Completed")
            | 'Log After Subscriptions' >> beam.ParDo(LogElementDoFn(), step_name='Subscriptions')
        )

        # Step 3: Additional transformations with logging for each step
        transformed_subs_1 = (
            transformed_subscriptions
            | 'Subs_1' >> beam.Map(lambda x: x + " -> Subs_1 Completed")
            | 'Log After Subs_1' >> beam.ParDo(LogElementDoFn(), step_name='Subs_1')
        )
        transformed_subs_2 = (
            transformed_subscriptions
            | 'Subs_2' >> beam.Map(lambda x: x + " -> Subs_2 Completed")
            | 'Log After Subs_2' >> beam.ParDo(LogElementDoFn(), step_name='Subs_2')
        )
        transformed_subs_3 = (
            transformed_subscriptions
            | 'Subs_3' >> beam.Map(lambda x: x + " -> Subs_3 Completed")
            | 'Log After Subs_3' >> beam.ParDo(LogElementDoFn(), step_name='Subs_3')
        )

        # Step 4: Other transformations with logging
        transformed_orders = (
            input_data
            | 'Orders' >> beam.Map(lambda x: x + " -> Orders Completed")
            | 'Log After Orders' >> beam.ParDo(LogElementDoFn(), step_name='Orders')
        )
        transformed_offers = (
            input_data
            | 'Offers' >> beam.Map(lambda x: x + " -> Offers Completed")
            | 'Log After Offers' >> beam.ParDo(LogElementDoFn(), step_name='Offers')
        )

        # Step 5: Merge all collections and log final transformation
        final_data = (
            [transformed_subs_1, transformed_subs_2, transformed_subs_3, transformed_orders, transformed_offers]
            | 'Flatten Merged Collections' >> beam.Flatten()
            | 'Final Transformation' >> beam.Map(lambda x: x + " -> Final Transformation Completed")
            | 'Log Final Data' >> beam.ParDo(LogElementDoFn(), step_name='Final Transformation')
        )

        # Optional output step (comment out if not writing to storage)
        final_data | 'Write Output' >> beam.io.WriteToText('output.txt')

if __name__ == '__main__':
    run_pipeline()
