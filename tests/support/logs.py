"""The JSON lines tandem writes through logline, read back by event and logger."""

import io
import json
from typing import Any

from tests.support.waiting import until

type Line = dict[str, Any]


class LogLines:
    """Every JSON line written to the stdout the fixture replaced."""

    def __init__(self, stdout: io.StringIO) -> None:
        self._stdout = stdout

    def all(self) -> list[Line]:
        return [json.loads(text) for text in self._stdout.getvalue().splitlines()]

    def of(self, event: str, logger: str) -> list[Line]:
        return [line for line in self.all() if line["event"] == event and line["logger"] == logger]

    async def await_of(self, event: str, logger: str, *, count: int = 1) -> list[Line]:
        """The lines of `event`, once at least `count` of them are written."""

        async def written() -> bool:
            return len(self.of(event, logger)) >= count

        await until(written)
        return self.of(event, logger)

    def of_logger(self, logger: str) -> list[Line]:
        return [line for line in self.all() if line["logger"] == logger]

    def assert_none_holds(self, secret: str) -> None:
        """No tandem line carries `secret`, such as a generation or a payload."""
        tandem_lines = [line for line in self.all() if line["logger"].startswith("tandem.")]
        assert not [line for line in tandem_lines if secret in json.dumps(line)]
