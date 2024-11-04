FROM apache/beam_python3.9_sdk:latest

# Install system dependencies, including Graphviz
RUN apt-get update && apt-get install -y graphviz

# Set the working directory
WORKDIR /workspace

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install Python dependencies including Graphviz
RUN pip install -r requirements.txt

# Optionally, if you want to keep this container running
# CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
