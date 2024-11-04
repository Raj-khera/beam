import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import graphviz

def run_pipeline():
    # Set up pipeline options
    options = PipelineOptions(
        runner='DirectRunner',
        save_main_session=True,
        staging_location='/tmp'  # Specify a staging location if needed
    )

    with beam.Pipeline(options=options) as pipeline:
        # Your Beam pipeline code here
        (pipeline
         | 'Create' >> beam.Create([1, 2, 3])
         | 'Print' >> beam.Map(print))

    return pipeline

def visualize_pipeline(pipeline):
    # Create a Graphviz Digraph
    dot = graphviz.Digraph()

    # Manually add nodes and edges based on your pipeline's transforms
    # This is a simple example; you'll need to adjust it to fit your specific pipeline
    dot.node('A', 'Create')
    dot.node('B', 'Print')

    # Connect the nodes
    dot.edge('A', 'B')

    # Render the graph to a file
    dot.render('pipeline_graph', format='png', cleanup=True)
    print("Graph has been rendered to pipeline_graph.png")

# Run the pipeline
pipeline = run_pipeline()

# Visualize the pipeline
visualize_pipeline(pipeline)
