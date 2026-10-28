import io
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import prompts  # noqa: E402


def make(answers, **kw):
    out = io.StringIO()
    fn = mock.Mock(side_effect=list(answers))
    return prompts.Prompter(input_fn=fn, out=out, **kw), fn, out


def positive_int(value):
    if not value.isdigit() or int(value) <= 0:
        raise ValueError("需要正整数")
    return int(value)


class TextTests(unittest.TestCase):
    def test_default_on_enter(self):
        p, fn, _ = make([""])
        self.assertEqual(p.text("名称", default="abc"), "abc")
        self.assertEqual(fn.call_args[0][0], "名称 [abc]: ")

    def test_validate_and_reask(self):
        p, fn, out = make(["x", "-1", "42"])
        self.assertEqual(p.text("数量", validate=positive_int), 42)
        self.assertEqual(fn.call_count, 3)
        self.assertEqual(out.getvalue().count("输入无效：需要正整数"), 2)

    def test_empty(self):
        p, _, out = make(["", "v"])
        self.assertEqual(p.text("值"), "v")
        self.assertIn("不能为空", out.getvalue())
        p, _, _ = make(["  "])
        self.assertIsNone(p.text("值", allow_empty=True))


class ChoiceTests(unittest.TestCase):
    OPTS = [("a", "Alpha"), ("b", "Beta"), ("c", "Gamma")]

    def test_number_value_default(self):
        p, _, out = make(["2"])
        self.assertEqual(p.choice("选择", self.OPTS), "b")
        self.assertIn("  1) Alpha", out.getvalue())
        p, _, _ = make(["c"])
        self.assertEqual(p.choice("选择", self.OPTS), "c")
        p, fn, out = make([""])
        self.assertEqual(p.choice("选择", self.OPTS, default="b"), "b")
        self.assertEqual(fn.call_args[0][0], "请输入序号 [2]: ")
        self.assertIn("2) Beta（默认）", out.getvalue())

    def test_reask(self):
        p, fn, out = make(["9", "", "zz", "1"])
        self.assertEqual(p.choice("选择", self.OPTS), "a")
        self.assertEqual(fn.call_count, 4)
        self.assertEqual(out.getvalue().count("输入无效"), 3)

    def test_free_text(self):
        def upper(v):
            if not v.startswith("x"):
                raise ValueError("必须以 x 开头")
            return v.upper()
        p, _, out = make(["bad", "xyz"])
        self.assertEqual(p.choice("选择", self.OPTS, free_text=upper), "XYZ")
        self.assertIn("必须以 x 开头", out.getvalue())


class ConfirmTests(unittest.TestCase):
    def test_answers(self):
        for answer, expected in (("", False), ("y", True), ("YES", True), ("n", False)):
            p, fn, _ = make([answer])
            self.assertIs(p.confirm("继续？"), expected, answer)
        self.assertEqual(fn.call_args[0][0], "继续？ [y/N]: ")
        p, fn, out = make(["maybe", "y"])
        self.assertTrue(p.confirm("继续？"))
        self.assertIn("输入无效", out.getvalue())


class AbortTests(unittest.TestCase):
    def test_eof_and_ctrl_c(self):
        for exc in (EOFError, KeyboardInterrupt):
            p, _, _ = make([exc()])
            with self.assertRaises(prompts.PromptAborted):
                p.text("值")
            p, _, _ = make([exc()])
            with self.assertRaises(prompts.PromptAborted):
                p.choice("选择", [("a", "A")])
            p, _, _ = make([exc()])
            with self.assertRaises(prompts.PromptAborted):
                p.confirm("继续？")
            gp = mock.Mock(side_effect=exc())
            with self.assertRaises(prompts.PromptAborted):
                prompts.Prompter(getpass_fn=gp, out=io.StringIO()).secret("key")

    def test_too_many_invalid(self):
        p, fn, _ = make(["x"] * 5)
        with self.assertRaises(prompts.PromptError):
            p.text("数量", validate=positive_int)
        self.assertEqual(fn.call_count, 5)


class SecretAndIntroTests(unittest.TestCase):
    def test_secret_not_echoed(self):
        out = io.StringIO()
        gp = mock.Mock(return_value="TOPSECRET")
        p = prompts.Prompter(getpass_fn=gp, out=out)
        self.assertEqual(p.secret("ACXD API key"), "TOPSECRET")
        gp.assert_called_once_with("ACXD API key: ")
        self.assertNotIn("TOPSECRET", out.getvalue())

    def test_intro_once(self):
        p, _, out = make(["a", "b"], intro="INTRO")
        p.text("一")
        p.text("二")
        self.assertEqual(out.getvalue().count("INTRO"), 1)

    def test_resolves_input_at_call_time(self):
        p = prompts.Prompter(out=io.StringIO())
        with mock.patch("builtins.input", return_value="v") as inp:
            self.assertEqual(p.text("值"), "v")
        inp.assert_called_once_with("值: ")


if __name__ == "__main__":
    unittest.main()
