"""Tiny MapReduce-like word count engine (no Hadoop, no Spark).
Phases: Input splits -> Map (parallel) -> Combine -> Partition -> Shuffle & Sort -> Reduce -> output files.
Usage: python mapreduce.py [input_dir] [output_dir]
"""
import os, re, sys
from multiprocessing import Pool

NUM_REDUCERS = 2
STOP = {"the", "was", "in", "a", "to", "and", "plus", "of"}  # tiny stop-word list


def read_splits(folder):
    """INPUT: each file in the folder is one split."""
    return [os.path.join(folder, f) for f in sorted(os.listdir(folder))
            if os.path.isfile(os.path.join(folder, f))]


def partition(word):
    """Deterministic partitioner (Python's hash() is randomised per process)."""
    return sum(ord(c) for c in word) % NUM_REDUCERS


def map_phase(path):
    """MAP one split: emit (word, 1). Then COMBINE locally and PARTITION."""
    pairs = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            for w in re.findall(r"[a-z0-9']+", line.lower()):
                if w not in STOP:
                    pairs.append((w, 1))                 # emit (word, 1)
    combined = {}                                         # COMBINE: local subtotals
    for w, n in pairs:
        combined[w] = combined.get(w, 0) + n
    buckets = [[] for _ in range(NUM_REDUCERS)]           # PARTITION to reducers
    for w, n in combined.items():
        buckets[partition(w)].append((w, n))
    print(f"[mapper pid={os.getpid()}] {os.path.basename(path)}: "
          f"{len(pairs)} pairs -> {len(combined)} after combine", flush=True)
    return buckets


def shuffle_sort(all_buckets, r):
    """SHUFFLE: collect reducer r's pairs from every mapper, SORT by key, group."""
    pairs = []
    for buckets in all_buckets:
        pairs.extend(buckets[r])
    pairs.sort(key=lambda p: p[0])
    groups, i = [], 0
    while i < len(pairs):                                 # group equal adjacent keys
        key, vals = pairs[i][0], []
        while i < len(pairs) and pairs[i][0] == key:
            vals.append(pairs[i][1]); i += 1
        groups.append((key, vals))
    return groups


def reduce_phase(groups):
    """REDUCE: sum values for each word."""
    return [(k, sum(v)) for k, v in groups]


def main():
    in_dir = sys.argv[1] if len(sys.argv) > 1 else "input"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    os.makedirs(out_dir, exist_ok=True)
    splits = read_splits(in_dir)
    with Pool(processes=len(splits)) as pool:             # mappers run at the same time
        all_buckets = pool.map(map_phase, splits)
    for r in range(NUM_REDUCERS):
        result = reduce_phase(shuffle_sort(all_buckets, r))
        with open(os.path.join(out_dir, f"part-r-{r:05d}"), "w") as f:
            for w, n in result:
                f.write(f"{w}\t{n}\n")
        print(f"[reducer {r}] wrote {len(result)} words")


if __name__ == "__main__":
    main()
