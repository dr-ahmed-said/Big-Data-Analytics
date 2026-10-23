"""Streamlit UI for the hand-built MapReduce engine.

CIS 41 03 - Big Data Analytics - Lecture 06
Assignment (Bonus): Build Your MapReduce

Run with:   streamlit run app.py
"""

import os
import sqlite3
import tempfile

import streamlit as st

from mapreduce_engine import (
    ETI_CLAIM_NOTES,
    ETI_EXPECTED,
    map_function,
    partition_function,
    partition_label,
    run_job,
)

st.set_page_config(page_title="Build Your MapReduce",
                   page_icon="M",
                   layout="wide")

PHASES = ["1 · Map", "2 · Combine", "3 · Partition", "4 · Shuffle & Sort", "5 · Reduce"]

st.markdown(
    """
    <style>
      .block-container { padding-top: 2.2rem; max-width: 1250px; }
      .mr-title { font-size: 2rem; font-weight: 700; letter-spacing: -0.02em;
                  margin-bottom: 0.1rem; }
      .mr-sub   { opacity: 0.65; font-size: 0.95rem; margin-bottom: 1.4rem; }
      .mr-flow  { display: flex; gap: 0.45rem; flex-wrap: wrap; margin: 0.2rem 0 1.4rem 0; }
      .mr-step  { padding: 0.32rem 0.8rem; border-radius: 999px; font-size: 0.82rem;
                  border: 1px solid rgba(128,128,128,0.35); white-space: nowrap; }
      .mr-arrow { opacity: 0.4; align-self: center; font-size: 0.8rem; }
      .mr-pair  { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
                  font-size: 0.82rem; }
      .mr-note  { opacity: 0.6; font-size: 0.85rem; }
      div[data-testid="stMetricValue"] { font-size: 1.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="mr-title">Build Your MapReduce</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="mr-sub">A MapReduce-like engine written by hand &mdash; no Hadoop, no Spark. '
    'CIS 41 03 &middot; Lecture 06 &middot; Mahmoud Khaled Mahmoud Yassin Badr &middot; 231005511</div>',
    unsafe_allow_html=True,
)

flow = '<span class="mr-arrow">&rarr;</span>'.join(
    f'<span class="mr-step">{p}</span>' for p in PHASES
)
st.markdown(f'<div class="mr-flow">{flow}</div>', unsafe_allow_html=True)


# --------------------------------------------------------------------------
# Sidebar: the job configuration
# --------------------------------------------------------------------------
with st.sidebar:
    st.subheader("Job settings")

    num_reducers = st.slider("Reducers", 1, 4, 2,
                             help="Number of partitions = number of reducers. "
                                  "One output file per reducer.")
    use_combiner = st.toggle("Use the combiner", value=True,
                             help="A mini-reducer on the mapper's own node. "
                                  "Only for speed: the answer is the same either way.")
    delay = st.slider("Work per mapper (seconds)", 0.0, 1.0, 0.3, 0.1,
                      help="Pretend each split is big, so the parallel run is visible.")

    st.divider()
    st.subheader("Input splits")
    st.caption("One text file = one split = one mapper.")

    uploaded = st.file_uploader("Upload .txt files", type="txt",
                                accept_multiple_files=True)

    if uploaded:
        splits = [(f.name, f.read().decode("utf-8", "replace")) for f in uploaded]
    else:
        text = st.text_area(
            "Or edit the claim notes (one split per line)",
            value="\n".join(t for _, t in ETI_CLAIM_NOTES),
            height=130,
        )
        splits = [(f"note_{i:02d}.txt", line)
                  for i, line in enumerate(text.splitlines(), start=1)
                  if line.strip()]

    st.caption(f"{len(splits)} split(s) &rarr; {len(splits)} mapper(s)")

if not splits:
    st.warning("Add at least one split in the sidebar.")
    st.stop()


# --------------------------------------------------------------------------
# Run the job
# --------------------------------------------------------------------------
out_dir = os.path.join(tempfile.gettempdir(), "mapreduce_ui_output")
job = run_job(splits, out_dir, num_reducers=num_reducers,
              use_combiner=use_combiner, delay=delay)

mappers = job["mappers"]
reducers = job["reducers"]
result = job["result"]

emitted = sum(len(m["pairs"]) for m in mappers)
after_combine = sum(len(m["combined"]) for m in mappers)
total_words = 0
for w in result:
    total_words += result[w]

serial = delay * len(mappers)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Splits / mappers", len(mappers))
c2.metric("Pairs emitted", emitted)
c3.metric("After combine", after_combine,
          delta=f"-{emitted - after_combine} over the network" if use_combiner else "combiner off",
          delta_color="normal" if use_combiner else "off")
c4.metric("Map wall time", f"{job['map_wall']:.2f} s",
          delta=f"{serial:.1f} s if one after another" if serial else None,
          delta_color="off")
c5.metric("Words counted", total_words)


# --------------------------------------------------------------------------
# The five phases
# --------------------------------------------------------------------------
tabs = st.tabs(PHASES + ["Result", "vs SQL GROUP BY"])


def pairs_html(pairs, limit=40):
    shown = "  ".join(f"({k}, {v})" for k, v in pairs[:limit])
    if len(pairs) > limit:
        shown += f"  … +{len(pairs) - limit} more"
    return f'<div class="mr-pair">{shown}</div>'


# 1 - Map ------------------------------------------------------------------
with tabs[0]:
    st.markdown("Each split is read record by record. For every word the mapper "
                "**emits `(word, 1)`**. Several pairs may share the same key — "
                "grouping is not the mapper's job.")
    for i, m in enumerate(mappers, start=1):
        with st.container(border=True):
            st.markdown(f"**Mapper {i}** · `{m['name']}` · {len(m['pairs'])} pairs")
            st.markdown(pairs_html(m["pairs"]), unsafe_allow_html=True)

    st.markdown('<div class="mr-note">For one record a mapper may emit no pair '
                '(filtering) or many pairs (demultiplexing).</div>',
                unsafe_allow_html=True)

# 2 - Combine --------------------------------------------------------------
with tabs[1]:
    if use_combiner:
        st.markdown("A **mini-reducer on the mapper's own node**: it sums the values "
                    "for each key locally, so less data travels over the network.")
        for i, m in enumerate(mappers, start=1):
            with st.container(border=True):
                st.markdown(f"**Mapper {i}** · {len(m['pairs'])} pairs &rarr; "
                            f"**{len(m['combined'])} pairs**")
                st.markdown(pairs_html(m["combined"]), unsafe_allow_html=True)
        st.success(f"{emitted} pairs became {after_combine} before the shuffle.")
    else:
        st.info("The combiner is off. The job still gives the same answer — "
                "it exists only for speed, and the framework may skip it.")

    st.markdown('<div class="mr-note">Safe for sum, count, max and min. '
                'Not safe for average: a combiner sees only part of the data, '
                'and an average of averages is wrong.</div>',
                unsafe_allow_html=True)

# 3 - Partition ------------------------------------------------------------
with tabs[2]:
    st.markdown(f"One partition per reducer (**{num_reducers}**). All records with "
                "the same key go to the same partition, so to the same reducer.")

    cols = st.columns(len(mappers))
    for col, (i, m) in zip(cols, enumerate(mappers, start=1)):
        with col:
            st.markdown(f"**Mapper {i}**")
            for r, part in enumerate(m["partitions"]):
                label = partition_label(r, num_reducers)
                st.markdown(f"<div class='mr-note'>&rarr; Reducer {r + 1} ({label})</div>"
                            + pairs_html(part), unsafe_allow_html=True)

    keys = sorted(result)
    st.dataframe(
        {"word": keys,
         "reducer": [f"R{partition_function(k, num_reducers) + 1}"
                     for k in keys]},
        use_container_width=True, hide_index=True)

# 4 - Shuffle & Sort -------------------------------------------------------
with tabs[3]:
    st.markdown("The partitions from **all** mappers are copied over the network to "
                "the reducer nodes, then grouped and sorted by key into "
                "`key -> list of values`. The only phase that crosses the network in bulk.")
    for red in reducers:
        r = red["reducer"]
        with st.container(border=True):
            st.markdown(f"**Reducer {r + 1}** ({partition_label(r, num_reducers)}) · "
                        f"received {len(red['received'])} pairs")
            st.markdown(pairs_html(red["received"]), unsafe_allow_html=True)
            grouped_txt = "     ".join(f"{k} &rarr; {v}" for k, v in red["grouped"].items())
            st.markdown(f'<div class="mr-pair">{grouped_txt}</div>', unsafe_allow_html=True)

# 5 - Reduce ---------------------------------------------------------------
with tabs[4]:
    st.markdown("For each key the reducer walks its list of values and writes one "
                "new pair. **One output file per reducer**, exactly as MapReduce "
                "writes `part-00000`, `part-00001` …")
    cols = st.columns(len(reducers))
    for col, red in zip(cols, reducers):
        with col:
            name = os.path.basename(red["path"])
            body = open(red["path"]).read()
            st.markdown(f"**`{name}`**")
            st.code(body or "(empty)", language="text")
            st.download_button("Download", body, file_name=name,
                               use_container_width=True, key=f"dl_{name}")

# Result -------------------------------------------------------------------
with tabs[5]:
    words = sorted(result)
    st.dataframe(
        {"word": words,
         "count": [result[w] for w in words],
         "reducer": [f"R{partition_function(w, num_reducers) + 1}"
                     for w in words]},
        use_container_width=True, hide_index=True)

    st.bar_chart({"word": words, "count": [result[w] for w in words]},
                 x="word", y="count", x_label="", y_label="count")

    st.caption(f"The counts add up to {total_words}, exactly the words in the input.")

    if result == ETI_EXPECTED:
        st.success("Self-check passed — same numbers as the lecture walk-through "
                   "and Lab 06: car 3, damage 3, fire 1, roof 1, theft 1, water 3.")
    else:
        st.info("Running on your own input, so there is nothing to compare with "
                "the lecture's 3 ETI claim notes.")

# vs SQL -------------------------------------------------------------------
with tabs[6]:
    st.markdown("**GROUP BY never went away.** MapReduce is the same aggregation, "
                "written out by hand so it can run on a hundred machines.")

    col_sql, col_mr = st.columns(2)
    with col_sql:
        st.markdown("**The database way**")
        st.code("SELECT word, COUNT(*)\nFROM words\nGROUP BY word;", language="sql")
    with col_mr:
        st.markdown("**The MapReduce way**")
        st.markdown("""
| SQL | Phase |
|---|---|
| `SELECT word` | 1 · Map |
| `GROUP BY word` | 4 · Shuffle & Sort |
| `COUNT(*)` | 5 · Reduce |
""")

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE words (word TEXT)")
    for _, text in splits:
        for line in text.splitlines():
            for w, _one in map_function(line):
                con.execute("INSERT INTO words VALUES (?)", (w,))
    rows = con.execute(
        "SELECT word, COUNT(*) FROM words GROUP BY word ORDER BY word").fetchall()

    st.dataframe(
        {"word": [w for w, _ in rows],
         "SQL": [n for _, n in rows],
         "MapReduce": [result.get(w, 0) for w, _ in rows]},
        use_container_width=True, hide_index=True)

    if {w: n for w, n in rows} == result:
        st.success("Both engines give the same answer.")
    else:
        st.error("The two answers differ.")


st.divider()
st.caption("The grouping and counting are all own code — plain dicts and loops. "
           "No collections.Counter, no itertools.groupby, no pandas. "
           "sqlite3 appears only as an independent cross-check, never to compute the result.")
