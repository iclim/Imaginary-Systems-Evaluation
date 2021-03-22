# Imaginary-Systems-Service-Evaluation

This repo contains:

1. A folder containing 5 logs of data named 'alpha.log', 'beta.log', ... 'epsilon.log' containing JSON delimited objects.

2. A Python file 'service_diagnostic_toolkit.py' (sdt) with eight functions to parse each log and extract data.

3. A JupyterLab notebook 'sdt_function_demo.ipynb' that serves as an environment to test each function.

To use the toolkit file, it is written assuming all 3 items listed above are saved in the same location. If files are not in the same folder, one would need to edit the 'extract_logs()' function to change the file path, ensure your IDE can import the toolkit file, etc... 
