"""A small MapReduce-like engine, written by hand.

Map -> Combine -> Partition -> Shuffle & Sort -> Reduce

Same five functions as bonus_mapreduce.ipynb, in one importable file so the
Streamlit app (app.py) and the notebook run exactly the same code.

The one rule: the grouping and counting are our own code, plain dicts and
loops. No collections.Counter, no itertools.groupby, no pandas.
"""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

DEFAULT_REDUCERS = 2


# --------------------------------------------------------------------------
# Phase 1 - Map
# --------------------------------------------------------------------------
def map_function(line):
    """Map: one record in, a list of (word, 1) pairs out."""
    pairs = []
    for word in re.findall(r"[a-z']+", line.lower()):   # split the record into words
        pairs.append((word, 1))                         # emit (key, value)
    return pairs


# --------------------------------------------------------------------------
# Phase 2 - Combine (optional)
# --------------------------------------------------------------------------
def combine_function(pairs):
    """Combine: local subtotals on the mapper's own node."""
    local = {}                      # key -> list of values
    for key, value in pairs:
        if key not in local:
            local[key] = []
        local[key].append(value)

    combined = []
    for key in local:               # apply the reduce logic locally: sum
        total = 0
        for v in local[key]:
            total += v
        combined.append((key, total))
    return combined


# --------------------------------------------------------------------------
# Phase 3 - Partition
# --------------------------------------------------------------------------
def partition_function(key, num_reducers=DEFAULT_REDUCERS):
    """Partition: return the reducer number for this key."""
    if num_reducers == 2:
        return 0 if key[0] <= "f" else 1    # Reducer 1 = a-f, Reducer 2 = g-z
    # general fallback: hash the key, so equal keys still land on one reducer
    return sum(ord(c) for c in key) % num_reducers


def partition_label(reducer_id, num_reducers=DEFAULT_REDUCERS):
    """The word range a reducer is responsible for, for display only."""
    if num_reducers == 2:
        return "a-f" if reducer_id == 0 else "g-z"
    return f"hash % {num_reducers} == {reducer_id}"


# --------------------------------------------------------------------------
# Phase 4 - Shuffle & Sort
# --------------------------------------------------------------------------
def shuffle_and_sort(all_partitions, reducer_id):
    """Shuffle: collect this reducer's partition from every mapper.
    Sort: group by key, in sorted order."""
    received = []
    for partitions in all_partitions:
        received += partitions[reducer_id]

    grouped = {}
    for key, value in received:
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(value)

    return received, {k: grouped[k] for k in sorted(grouped)}


# --------------------------------------------------------------------------
# Phase 5 - Reduce
# --------------------------------------------------------------------------
def reduce_function(key, values):
    """Reduce: sum the list of values for one key."""
    total = 0
    for v in values:
        total += v
    return (key, total)


def reduce_task(reducer_id, grouped, output_dir):
    """One reduce task: reduce every key, write part-0000N."""
    path = os.path.join(output_dir, f"part-{reducer_id:05d}")
    with open(path, "w") as f:
        for key in grouped:                     # already sorted
            k, total = reduce_function(key, grouped[key])
            f.write(f"{k}\t{total}\n")
    return path


# --------------------------------------------------------------------------
# The whole job
# --------------------------------------------------------------------------
def run_job(splits, output_dir, num_reducers=DEFAULT_REDUCERS,
            use_combiner=True, delay=0.0):
    """Run one MapReduce job and report what every phase produced.

    splits    : list of (name, text) - one split per text file
    output_dir: where part-00000, part-00001 ... are written
    delay     : seconds each mapper pretends to work, so the overlap is visible

    Returns a dict with the per-phase detail the UI shows.
    """
    os.makedirs(output_dir, exist_ok=True)

    def one_map_task(split):
        name, text = split
        t_start = time.time()

        pairs = []
        for line in text.splitlines():
            pairs += map_function(line)                     # 1 - Map

        if delay:
            time.sleep(delay)

        combined = combine_function(pairs) if use_combiner else pairs   # 2 - Combine

        parts = [[] for _ in range(num_reducers)]           # 3 - Partition
        for key, value in combined:
            parts[partition_function(key, num_reducers)].append((key, value))

        return {
            "name": name,
            "thread": _thread_name(),
            "pairs": pairs,
            "combined": combined,
            "partitions": parts,
            "start": t_start,
            "end": time.time(),
        }

    # map task: one per split, all at the same time
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max(1, len(splits))) as pool:
        mappers = list(pool.map(one_map_task, splits))
    map_wall = time.time() - t0

    all_partitions = [m["partitions"] for m in mappers]

    # reduce task: one per reducer, also at the same time
    def one_reduce_task(r):
        received, grouped = shuffle_and_sort(all_partitions, r)   # 4 - Shuffle & Sort
        path = reduce_task(r, grouped, output_dir)                # 5 - Reduce
        return {"reducer": r, "received": received,
                "grouped": grouped, "path": path}

    t1 = time.time()
    with ThreadPoolExecutor(max_workers=num_reducers) as pool:
        reducers = list(pool.map(one_reduce_task, range(num_reducers)))
    reduce_wall = time.time() - t1

    # the final counts, read back out of the part files
    result = {}
    for red in reducers:
        for line in open(red["path"]):
            word, count = line.split("\t")
            result[word] = int(count)

    return {
        "mappers": mappers,
        "reducers": reducers,
        "result": result,
        "map_wall": map_wall,
        "reduce_wall": reduce_wall,
        "num_reducers": num_reducers,
        "use_combiner": use_combiner,
    }


def _thread_name():
    import threading
    return threading.current_thread().name


# --------------------------------------------------------------------------
# The 3 ETI claim notes from the lecture
# --------------------------------------------------------------------------
ETI_CLAIM_NOTES = [
    ("note_01.txt", "car fire car damage"),
    ("note_02.txt", "water roof water damage"),
    ("note_03.txt", "car theft water damage"),
]

ETI_EXPECTED = {"car": 3, "damage": 3, "fire": 1, "roof": 1, "theft": 1, "water": 3}
