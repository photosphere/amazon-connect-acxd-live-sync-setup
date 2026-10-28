"""Interactive prompt primitives for deploy.py (no business logic, no deploy import).

Every prompt re-asks on invalid input (at most max_attempts times, then PromptError) and turns
EOF / Ctrl-C into PromptAborted.  User-facing text is Chinese, like the rest of the CLI.
"""

from __future__ import annotations

import builtins
import getpass
import sys


class PromptAborted(Exception):
    """The user pressed Ctrl-C or sent EOF at a prompt."""


class PromptError(Exception):
    """Too many invalid answers in a row."""


class Prompter:
    def __init__(self, input_fn=None, getpass_fn=None, out=None, max_attempts=5, intro=None):
        # None = resolve builtins.input / getpass.getpass / sys.stdout at call time, so tests can
        # patch them and contextlib.redirect_stdout keeps working.
        self._input_fn = input_fn
        self._getpass_fn = getpass_fn
        self._out = out
        self.max_attempts = max_attempts
        self._intro = intro
        self._intro_done = False

    # -- helpers ------------------------------------------------------------

    @property
    def out(self):
        return self._out if self._out is not None else sys.stdout

    def _print(self, text: str = "") -> None:
        print(text, file=self.out, flush=True)

    def _show_intro(self) -> None:
        if self._intro and not self._intro_done:
            self._print(self._intro)
        self._intro_done = True

    def _read(self, prompt: str) -> str:
        self._show_intro()
        fn = self._input_fn or builtins.input
        try:
            return fn(prompt)
        except (EOFError, KeyboardInterrupt) as exc:
            raise PromptAborted() from exc

    def _invalid(self, reason: str) -> None:
        self._print(f"输入无效：{reason}")

    def _too_many(self, label: str):
        return PromptError(f"连续 {self.max_attempts} 次输入无效，已放弃：{label}")

    # -- primitives ---------------------------------------------------------

    def text(self, label: str, default: str | None = None, validate=None, allow_empty: bool = False):
        """Free text.  validate(value) returns the normalized value or raises ValueError(reason).

        Enter returns the default (not re-validated), or None when allow_empty and no default.
        """
        suffix = f" [{default}]" if default not in (None, "") else ""
        for _ in range(self.max_attempts):
            value = self._read(f"{label}{suffix}: ").strip()
            if not value:
                if default not in (None, ""):
                    return default
                if allow_empty:
                    return None
                self._invalid("不能为空")
                continue
            if validate is None:
                return value
            try:
                return validate(value)
            except ValueError as exc:
                self._invalid(str(exc))
        raise self._too_many(label)

    def choice(self, label: str, options: list, default=None, free_text=None, prompt: str = "请输入序号"):
        """Numbered pick-list.  options: [(value, display)].

        Accepts the number, the exact value, or (when free_text is given) any input free_text(value)
        accepts; free_text returns the normalized value or raises ValueError(reason).
        """
        values = [v for v, _ in options]
        default_index = values.index(default) + 1 if default is not None and default in values else None
        self._show_intro()
        self._print(label)
        for i, (_, display) in enumerate(options, 1):
            mark = "（默认）" if i == default_index else ""
            self._print(f"  {i}) {display}{mark}")
        suffix = f" [{default_index}]" if default_index else ""
        for _ in range(self.max_attempts):
            raw = self._read(f"{prompt}{suffix}: ").strip()
            if not raw:
                if default_index:
                    return values[default_index - 1]
                self._invalid("请选择一项")
                continue
            if raw.isdigit():
                n = int(raw)
                if 1 <= n <= len(values):
                    return values[n - 1]
                self._invalid(f"序号必须在 1-{len(values)} 之间")
                continue
            exact = [v for v in values if str(v) == raw]
            if exact:
                return exact[0]
            if free_text is not None:
                try:
                    return free_text(raw)
                except ValueError as exc:
                    self._invalid(str(exc))
                    continue
            self._invalid(f"请输入 1-{len(values)} 之间的序号")
        raise self._too_many(label)

    def confirm(self, label: str, default: bool = False) -> bool:
        suffix = " [Y/n]" if default else " [y/N]"
        for _ in range(self.max_attempts):
            raw = self._read(f"{label}{suffix}: ").strip().lower()
            if not raw:
                return default
            if raw in ("y", "yes"):
                return True
            if raw in ("n", "no"):
                return False
            self._invalid("请输入 y 或 n")
        raise self._too_many(label)

    def info(self, text: str) -> None:
        """Informational text shown with the prompts (after the intro), e.g. a preview."""
        self._show_intro()
        self._print(text)

    def secret(self, label: str) -> str:
        """getpass: the value is never echoed or written to out."""
        self._show_intro()
        fn = self._getpass_fn or getpass.getpass
        try:
            return fn(f"{label}: ")
        except (EOFError, KeyboardInterrupt) as exc:
            raise PromptAborted() from exc
