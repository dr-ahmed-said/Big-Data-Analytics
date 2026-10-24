
import os
import re
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict


# 1. INPUT: Read all text files
def get_input_files(data_folder="data"):
    files = []

    if os.path.exists(data_folder):
        for f in os.listdir(data_folder):
            if f.endswith(".txt"):
                files.append(os.path.join(data_folder, f))

    return sorted(files)


# 2. MAP: Convert each word into (word, 1)
def mapper(file_path):
    word_pairs = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read().lower()
        words = re.findall(r"\b\w+\b", text)

        for word in words:
            word_pairs.append((word, 1))

    return word_pairs


# 3. COMBINE + PARTITION
def combine_and_partition(all_mapped_results, num_reducers=2):
    partitions = [[] for _ in range(num_reducers)]

    for file_pairs in all_mapped_results:
        local_counts = defaultdict(int)

        # Combine repeated words in this file
        for word, count in file_pairs:
            local_counts[word] += count

        # Send each word to one reducer
        for word, count in local_counts.items():
            reducer_id = hash(word) % num_reducers
            partitions[reducer_id].append((word, count))

    return partitions


# 4. SHUFFLE, SORT + 5. REDUCE
def reducer_worker(reducer_id, partition_data, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)

    grouped_words = defaultdict(list)

    # Shuffle: group values belonging to the same word
    for word, count in partition_data:
        grouped_words[word].append(count)

    # Sort words alphabetically
    sorted_words = sorted(grouped_words.keys())

    output_file = os.path.join(
        output_dir, f"output_reducer_{reducer_id}.txt"
    )

    # Reduce: add all values for each word
    with open(output_file, "w", encoding="utf-8") as f:
        for word in sorted_words:
            total_count = sum(grouped_words[word])
            f.write(f"{word}\t{total_count}\n")

    print(f"Reducer {reducer_id} finished: {output_file}")


# MAIN PROGRAM
def main():
    data_folder = "data"
    input_files = get_input_files(data_folder)

    print(f"Found {len(input_files)} input files: {input_files}")

    if not input_files:
        print("Error: No text files found in the data folder!")
        return

    # Run one mapper task per file, concurrently
    with ThreadPoolExecutor() as executor:
        all_mapped_results = list(
            executor.map(mapper, input_files)
        )

    # Combine local counts and partition into 2 reducers
    partitions = combine_and_partition(
        all_mapped_results, num_reducers=2
    )

    # Run both reducers
    for reducer_id, partition_data in enumerate(partitions):
        reducer_worker(reducer_id, partition_data)

    print("MapReduce simulation finished successfully!")


if __name__ == "__main__":
    main()
