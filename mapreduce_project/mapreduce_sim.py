from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import os



def map_file(file_path):
  mapped_data = []
  with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
      words = line.strip().split()
      for word in words:
        if word:
          mapped_data.append((word.lower(), 1))
  return mapped_data



def combine_and_partition(mapped_data, num_reducers=2):
  # Local aggregation (Combine)
  local_counts = defaultdict(int)
  for word, count in mapped_data:
    local_counts[word] += count

  
  partitions = [defaultdict(list) for _ in range(num_reducers)]
  for word, count in local_counts.items():
    reducer_id = hash(word) % num_reducers
    partitions[reducer_id][word].append(count)

  return partitions



def run_reducer(reducer_id, partition_data, output_dir):
  reduced_results = {}
  # Shuffle & Sort (Sort keys alphabetically)
  sorted_keys = sorted(partition_data.keys())
  for word in sorted_keys:
    # Sum up values from combiners
    reduced_results[word] = sum(partition_data[word])

  
  os.makedirs(output_dir, exist_ok=True)
  output_file = os.path.join(output_dir, f"reducer_{reducer_id}_output.txt")
  with open(output_file, "w", encoding="utf-8") as f:
    for word, total in reduced_results.items():
      f.write(f"{word}\t{total}\n")

  return output_file



def run_mapreduce(input_dir, output_dir):
  # 1. Input: Get all split files
  files = [
      os.path.join(input_dir, f)
      for f in os.listdir(input_dir)
      if f.endswith(".txt")
  ]
  print(f"[INFO] Found {len(files)} splits. Running Mappers in parallel...")

  # 3. Run Mappers in parallel using ProcessPoolExecutor
  with ProcessPoolExecutor() as executor:
    map_results = list(executor.map(map_file, files))

  print("[INFO] Mappers finished. Applying Combine and Partition...")

  
  num_reducers = 2
  combined_partitions = [defaultdict(list) for _ in range(num_reducers)]

  for mapped_data in map_results:
    partitions = combine_and_partition(mapped_data, num_reducers)
    for i in range(num_reducers):
      for word, counts in partitions[i].items():
        combined_partitions[i][word].extend(counts)

  print("[INFO] Running Reducers...")

  
  output_files = []
  for i in range(num_reducers):
    out_file = run_reducer(i, combined_partitions[i], output_dir)
    output_files.append(out_file)

  print(f"[INFO] MapReduce simulation completed! Output files: {output_files}")


if __name__ == "__main__":
  
  input_directory = "input_splits"
  output_directory = "output_results"

  os.makedirs(input_directory, exist_ok=True)
  # إنشاء ملفات تدريبية (Splits)
  with open(os.path.join(input_directory, "split1.txt"), "w") as f:
    f.write("car damage fire roof theft water\ncar car damage")
  with open(os.path.join(input_directory, "split2.txt"), "w") as f:
    f.write("fire water water theft roof roof")

  run_mapreduce(input_directory, output_directory)