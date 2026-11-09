"""Launch a local browser dashboard for the MapReduce word-count assignment."""

from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from pathlib import Path
import webbrowser


HERE = Path(__file__).resolve().parent
DEFAULT_OUTPUT = HERE / "output_results"
HOST, PORT = "127.0.0.1", 8765


def make_job(splits: list[dict], output_dir: Path) -> dict:
    if not splits:
        raise ValueError("Choose a folder containing at least one .txt file.")

    def map_split(split: dict) -> dict:
        tokens = [word.lower() for word in split["content"].split() if word]
        return {"file": split["name"], "tokens": len(tokens), "counts": Counter(tokens)}

    with ThreadPoolExecutor(max_workers=min(len(splits), 8)) as pool:
        mapper_rows = list(pool.map(map_split, splits))

    partitions = [defaultdict(list), defaultdict(list)]
    for row in mapper_rows:
        for word, count in row["counts"].items():
            partitions[hash(word) % 2][word].append(count)

    output_dir.mkdir(parents=True, exist_ok=True)
    reducers, total_counts = [], Counter()
    for reducer_id, partition in enumerate(partitions):
        words = {word: sum(values) for word, values in sorted(partition.items())}
        total_counts.update(words)
        output_file = output_dir / f"reducer_{reducer_id}_output.txt"
        output_file.write_text("".join(f"{word}\t{count}\n" for word, count in words.items()), encoding="utf-8")
        reducers.append({"id": reducer_id, "words": words, "file": output_file.name})

    return {
        "mappers": [
            {
                "file": row["file"],
                "tokens": row["tokens"],
                "counts": dict(row["counts"]),
                "routes": {word: hash(word) % 2 for word in row["counts"]},
            }
            for row in mapper_rows
        ],
        "reducers": reducers,
        "counts": dict(total_counts),
        "tokens": sum(row["tokens"] for row in mapper_rows),
        "output_dir": str(output_dir),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format, *_args):
        pass

    def _send(self, status: int, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/config":
            body = json.dumps({"output_dir": str(DEFAULT_OUTPUT)}).encode("utf-8")
            self._send(200, body, "application/json; charset=utf-8")
        elif self.path == "/" or self.path == "/index.html":
            self._send(200, (HERE / "gui.html").read_bytes(), "text/html; charset=utf-8")
        else:
            self._send(404, b"Not found", "text/plain; charset=utf-8")

    def do_POST(self):
        if self.path != "/run":
            self._send(404, b"Not found", "text/plain; charset=utf-8")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length).decode("utf-8"))
            output_dir = Path(request.get("output_dir") or DEFAULT_OUTPUT).expanduser()
            result = make_job(request.get("splits", []), output_dir)
            self._send(200, json.dumps(result).encode("utf-8"), "application/json; charset=utf-8")
        except (ValueError, OSError, json.JSONDecodeError) as error:
            self._send(400, json.dumps({"error": str(error)}).encode("utf-8"), "application/json; charset=utf-8")


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    url = f"http://{HOST}:{PORT}"
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    print(f"MapReduce dashboard opened at {url}. Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
