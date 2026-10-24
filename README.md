# MapReduce Engine Simulation in Python

This repository contains a simple, light-weight MapReduce simulation in Python built without external frameworks (No Hadoop, No Spark).

## Features & Implementation Phases
1. **Input**: Reads text files split from the `data/` folder as independent splits.
2. **Map**: Tokenizes input text into normalized lower-case words and emits `(word, 1)` key-value pairs.
3. **Parallel Execution**: Uses `ThreadPoolExecutor` to run mappers in parallel per split.
4. **Combine & Partition**: 
   - Performs local in-mapper subtotals (Combiner).
   - Partitions data using hash modulo into **2 Reducers**.
5. **Shuffle, Sort & Reduce**: Sorts unique keys alphabetically and aggregates total counts per word into separate output files (`output_reducer_0.txt` and `output_reducer_1.txt`).

## How to Run

1. Make sure Python 3.x is installed.
2. Place input files inside the `data/` directory.
3. Run the pipeline:
   ```bash
   python main.py