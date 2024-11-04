import apache_beam as beam
from apache_beam import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from graphviz import Digraph
from IPython.display import Image, display

# Set logging level
logging.basicConfig(level=logging.INFO)

class LogElementDoFn(DoFn):
    def process(self, element, step_name):
        # Log completion message with the step name
        logging.info(f"{step_name} completed with element: {element}")
        yield element  # Pass the element downstream without modification

# Create a Digraph object for visualization
def create_pipeline_graph():
    dot = Digraph(comment='Apache Beam Pipeline')
    dot.attr(rankdir='LR', size='16,10')  # Layout from left to right
    return dot

# Add pipeline steps to the Graphviz graph
def add_step(dot, step_name, label):
    dot.node(step_name, label=label, shape='rectangle')  # Set shape to rectangle

def add_edge(dot, from_step, to_step):
    dot.edge(from_step, to_step)

def run_pipeline():
    # Define pipeline options with the `flags=[]` argument to ignore Jupyter-specific args
    pipeline_options = PipelineOptions(flags=[], streaming=False, runner='DirectRunner')

    # Initialize Graphviz digraph
    dot = create_pipeline_graph()

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Create initial input data and log
        input_data = (
            p
            | 'Create Input' >> beam.Create(['Data inputted'])
            | 'Log Input Data' >> beam.ParDo(LogElementDoFn(), step_name='Input Data')
        )
        add_step(dot, 'Create Input', 'Create Input')
        
        # Step 2: First transformation with logging
        transformed_subscriptions = (
            input_data
            | 'Subscriptions' >> beam.Map(lambda x: x + " -> Subscriptions Completed")
            | 'Log After Subscriptions' >> beam.ParDo(LogElementDoFn(), step_name='Subscriptions')
        )
        add_step(dot, 'Subscriptions', 'Subscriptions')
        add_edge(dot, 'Create Input', 'Subscriptions')
        
        # Step 3: Additional transformations with logging for each step
        transformed_subs_1 = (
            transformed_subscriptions
            | 'Subs_1' >> beam.Map(lambda x: x + " -> Subs_1 Completed")
            | 'Log After Subs_1' >> beam.ParDo(LogElementDoFn(), step_name='Subs_1')
        )
        add_step(dot, 'Subs_1', 'Subs_1')
        add_edge(dot, 'Subscriptions', 'Subs_1')
        
        transformed_subs_2 = (
            transformed_subscriptions
            | 'Subs_2' >> beam.Map(lambda x: x + " -> Subs_2 Completed")
            | 'Log After Subs_2' >> beam.ParDo(LogElementDoFn(), step_name='Subs_2')
        )
        add_step(dot, 'Subs_2', 'Subs_2')
        add_edge(dot, 'Subscriptions', 'Subs_2')
        
        transformed_subs_3 = (
            transformed_subscriptions
            | 'Subs_3' >> beam.Map(lambda x: x + " -> Subs_3 Completed")
            | 'Log After Subs_3' >> beam.ParDo(LogElementDoFn(), step_name='Subs_3')
        )
        add_step(dot, 'Subs_3', 'Subs_3')
        add_edge(dot, 'Subscriptions', 'Subs_3')
        
        # Step 4: Other transformations with logging
        transformed_orders = (
            input_data
            | 'Orders' >> beam.Map(lambda x: x + " -> Orders Completed")
            | 'Log After Orders' >> beam.ParDo(LogElementDoFn(), step_name='Orders')
        )
        add_step(dot, 'Orders', 'Orders')
        add_edge(dot, 'Create Input', 'Orders')
        
        transformed_offers = (
            input_data
            | 'Offers' >> beam.Map(lambda x: x + " -> Offers Completed")
            | 'Log After Offers' >> beam.ParDo(LogElementDoFn(), step_name='Offers')
        )
        add_step(dot, 'Offers', 'Offers')
        add_edge(dot, 'Create Input', 'Offers')
        
        # Step 5: Merge all collections and log final transformation
        final_data = (
            [transformed_subs_1, transformed_subs_2, transformed_subs_3, transformed_orders, transformed_offers]
            | 'Flatten Merged Collections' >> beam.Flatten()
            | 'Final Transformation' >> beam.Map(lambda x: x + " -> Final Transformation Completed")
            | 'Log Final Data' >> beam.ParDo(LogElementDoFn(), step_name='Final Transformation')
        )
        add_step(dot, 'Flatten Merged Collections', 'Flatten Merged Collections')
        add_edge(dot, 'Subs_1', 'Flatten Merged Collections')
        add_edge(dot, 'Subs_2', 'Flatten Merged Collections')
        add_edge(dot, 'Subs_3', 'Flatten Merged Collections')
        add_edge(dot, 'Orders', 'Flatten Merged Collections')
        add_edge(dot, 'Offers', 'Flatten Merged Collections')
        
        add_step(dot, 'Final Transformation', 'Final Transformation')
        add_edge(dot, 'Flatten Merged Collections', 'Final Transformation')
        
        # Optional output step (comment out if not writing to storage)
        final_data | 'Write Output' >> beam.io.WriteToText('outputs/output.txt')
    
    # Render and save the graph
    output_path = 'outputs/beam_pipeline_diagram'
    dot.render(output_path, format='png', view=False)
    
    # Display the image in Jupyter
    #display(Image(filename=output_path))

if __name__ == '__main__':
    run_pipeline()
