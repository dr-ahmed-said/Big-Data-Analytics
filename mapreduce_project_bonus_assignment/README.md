MapReduce Simulation 

Description:
A custom Python simulation of a MapReduce framework for word count without using Hadoop or Spark. It processes multiple file splits using parallel Mappers, local Combining, Partitioning to 2 Reducers, Shuffle & Sort, and Reducer output generation.

How to Run:
py mapreduce_sim.py

Desktop GUI:
py gui.py

The GUI opens a local browser dashboard. Choose the folder containing your
input splits, run the word-count simulation, inspect mapper and combiner counts,
and view the sorted results written by both reducers. It uses Python's built-in
libraries and does not need extra packages. Stop the local dashboard with
Ctrl+C in the terminal.
