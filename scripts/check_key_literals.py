"""Fail when a string literal outside tandem.keys carries the `tandem:` key segment.

Every Redis key and channel tandem names is built in `tandem.keys` from that segment; Lua bodies
reach keys only through KEYS[].
"""

import ast
import sys
from pathlib import Path

KEY_SEGMENT = "tandem:"
SOURCE_DIRECTORY = Path(__file__).resolve().parent.parent / "src" / "tandem"
KEYS_MODULE = "keys.py"


def key_literal_lines(source_path: Path) -> list[int]:
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and KEY_SEGMENT in node.value
    ]


def main(source_directory: Path = SOURCE_DIRECTORY) -> int:
    offences = [
        f"{source_path}:{line}: key literal outside tandem.keys"
        for source_path in sorted(source_directory.glob("*.py"))
        if source_path.name != KEYS_MODULE
        for line in key_literal_lines(source_path)
    ]
    if offences:
        print("\n".join(offences), file=sys.stderr)
    return 1 if offences else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else SOURCE_DIRECTORY))
