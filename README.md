# Overview

This project is a React-based frontend and Python/FastAPI backend integration that allows users to create and manage node-based pipelines. The assignment focuses on the following tasks:

1. Node Abstraction: Implement an abstraction for creating different types of nodes (inputs, outputs, LLMs, text) with shared code for easier maintenance.

2. Styling: Apply a unified, appealing design across all components.

3. Text Node Logic: Add dynamic text input resizing and support for user-defined variables in the Text node.

4. Backend Integration: Integrate the frontend with a Python/FastAPI backend, allowing the submission of pipelines and returning details on the number of nodes, edges, and whether the pipeline forms a Directed Acyclic Graph (DAG).

## Features

1. Node Abstraction
- Implemented a common structure for nodes, reducing redundancy.
- Created five new nodes to demonstrate the flexibility of the abstraction.
- Abstracted components to allow easy future modifications and styling changes.

2. Styling
- Applied custom styles to improve the design and visual consistency of the application.
- Used modern CSS techniques and/or external libraries for a responsive and clean user interface.

3. Text Node Logic
- Text node dynamically resizes based on the input, improving user visibility.
- Users can define variables in the text field using double curly brackets {{variable}}. This creates a new handle (connection point) for the variable.

4. Backend Integration
- The frontend can now submit the nodes and edges of the pipeline to the backend for analysis.
- Backend checks the pipeline for:
    # Total number of nodes
    # Total number of edges
    # Whether the pipeline forms a Directed Acyclic Graph (DAG)
- Displays the result via a user-friendly alert on the frontend.

### Setup and Installation

1. Navigate to the /backend directory: cd backend
2. Run the FastAPI server: uvicorn main:app --reload

#### Usage

1. Create a pipeline by dragging and connecting nodes on the frontend.
2. Use the Text node to define variables by entering text like {{variable_name}} in the input field.
3. Once the pipeline is complete, click the "Submit" button to send the data to the backend.
4. A notification will pop up with the number of nodes, number of edges, and whether the pipeline forms a DAG.

##### Project Structure

/backend/
  ├── main.py            # FastAPI backend logic