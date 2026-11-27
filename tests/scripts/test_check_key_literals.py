import runpy
import sys
from pathlib import Path

import pytest

from scripts import check_key_literals

SCRIPT = Path(check_key_literals.__file__)


def test_a_key_literal_outside_the_keys_module_is_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "wake.py").write_text('CHANNEL = "p:tandem:v2:wake"\n', encoding="utf-8")

    assert check_key_literals.main(tmp_path) == 1
    assert "wake.py:1: key literal outside tandem.keys" in capsys.readouterr().err


def test_the_keys_module_and_modules_without_a_key_literal_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "keys.py").write_text('SEGMENT = "tandem:"\n', encoding="utf-8")
    (tmp_path / "wake.py").write_text('NAME = "wake"\nCOUNT = 3\n', encoding="utf-8")

    assert check_key_literals.main(tmp_path) == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("pass_directory", [True, False], ids=["given directory", "own sources"])
def test_the_program_exits_with_the_check_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pass_directory: bool
) -> None:
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), *([str(tmp_path)] if pass_directory else [])])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")

    assert exit_info.value.code == 0
