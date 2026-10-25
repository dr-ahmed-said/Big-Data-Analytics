
import os
import re
import zlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Configuration
INPUT_DIR = "input"
OUTPUT_DIR = "output"
NUM_REDUCERS = 2


# Phase 1: MAP
# One mapper processes one file

def mapper(file_path):
    local_pairs = []

    with open(file_path, "r", encoding="utf-8") as file:
        text = file.read().lower()

    words = re.findall(r"\b\w+\b", text)

    for word in words:
        local_pairs.append((word, 1))

    print(f"Mapper processed: {os.path.basename(file_path)}")

    return local_pairs


# Phase 2: COMBINE
# Sum repeated words locally

def combiner(pairs):
    local_counts = defaultdict(int)

    for word, count in pairs:
        local_counts[word] += count

    return list(local_counts.items())


# Phase 3: PARTITION
# Assign each word to one reducer


def partition(word):
    # Deterministic hash, unlike Python's built-in hash()
    return zlib.crc32(word.encode("utf-8")) % NUM_REDUCERS


# Phase 4: SHUFFLE AND SORT
# Group values by word and reducer


def shuffle_and_sort(combined_results):
    reducer_data = [
        defaultdict(list) for _ in range(NUM_REDUCERS)
    ]

    for word, count in combined_results:
        reducer_id = partition(word)
        reducer_data[reducer_id][word].append(count)

    # Sort the words assigned to each reducer
    sorted_reducers = []

    for data in reducer_data:
        sorted_data = {
            word: data[word]
            for word in sorted(data)
        }
        sorted_reducers.append(sorted_data)

    return sorted_reducers


# Phase 5: REDUCE
# Sum all values for each word

def reducer(reducer_id, word_data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_path = os.path.join(
        OUTPUT_DIR, f"reducer_{reducer_id}.txt"
    )

    with open(output_path, "w", encoding="utf-8") as output:
        for word, values in word_data.items():
            total = sum(values)
            output.write(f"{word}\t{total}\n")

    print(f"Reducer {reducer_id} wrote: {output_path}")


# DRIVER: Run all phases

def main():
    if not os.path.isdir(INPUT_DIR):
        raise FileNotFoundError(
            f"Input folder not found: {INPUT_DIR}"
        )

    # Every text file is one split
    files = sorted(
        os.path.join(INPUT_DIR, name)
        for name in os.listdir(INPUT_DIR)
        if name.lower().endswith(".txt")
        and os.path.isfile(os.path.join(INPUT_DIR, name))
    )

    if not files:
        raise ValueError("No .txt files found in the input folder.")

    print("\n--- MAP PHASE ---")

    # Run one mapper task per split, concurrently
    with ThreadPoolExecutor(max_workers=len(files)) as executor:
        mapper_results = list(executor.map(mapper, files))

    print("\n--- COMBINE PHASE ---")

    # Combine each mapper's output separately
    combined_results = []

    for pairs in mapper_results:
        combined_results.extend(combiner(pairs))

    print("Combined word counts:", combined_results)

    print("\n--- SHUFFLE AND SORT PHASE ---")

    reducer_data = shuffle_and_sort(combined_results)

    for reducer_id, data in enumerate(reducer_data):
        print(f"Reducer {reducer_id} receives: {dict(data)}")

    print("\n--- REDUCE PHASE ---")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Remove stale reducer output files from previous runs
    for reducer_id in range(NUM_REDUCERS):
        path = os.path.join(
            OUTPUT_DIR, f"reducer_{reducer_id}.txt"
        )
        if os.path.exists(path):
            os.remove(path)

    for reducer_id, data in enumerate(reducer_data):
        reducer(reducer_id, data)

    print("\nMapReduce job completed successfully!")


if __name__ == "__main__":
    main()
