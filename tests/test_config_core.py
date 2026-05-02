import asyncio
import contextlib
import io
import json
import os
import tempfile
import unittest

from core import config as config_mod


class ConfigCoreTests(unittest.TestCase):
    def setUp(self):
        self._old_config_path = config_mod.CONFIG_PATH

    def tearDown(self):
        config_mod.CONFIG_PATH = self._old_config_path

    def test_human_error_explains_incomplete_read(self):
        exc = asyncio.IncompleteReadError(partial=b"", expected=2)

        text = config_mod.human_error(exc)

        self.assertIn("соединение оборвалось", text)
        self.assertIn("proxy", text)

    def test_mask_sensitive_text_hides_proxy_credentials(self):
        text = "http://user:password@example.com:1234 failed"

        masked = config_mod.mask_sensitive_text(text)

        self.assertNotIn("user:password", masked)
        self.assertIn("***:***", masked)

    def test_load_config_reads_user_pair_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_mod.CONFIG_PATH = os.path.join(tmp, "config.json")
            with open(config_mod.CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump({"main_top_n": 321, "detail_top_n": 123}, f)

            cfg = config_mod.load_config()

        self.assertEqual(cfg["main_top_n"], 321)
        self.assertEqual(cfg["detail_top_n"], 123)

    def test_load_config_accepts_utf8_bom(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_mod.CONFIG_PATH = os.path.join(tmp, "config.json")
            with open(config_mod.CONFIG_PATH, "w", encoding="utf-8-sig") as f:
                json.dump({"main_top_n": 77, "detail_top_n": 55}, f)

            cfg = config_mod.load_config()

        self.assertEqual(cfg["main_top_n"], 77)
        self.assertEqual(cfg["detail_top_n"], 55)

    def test_ensure_config_preserves_existing_values_and_adds_missing_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_mod.CONFIG_PATH = os.path.join(tmp, "config.json")
            with open(config_mod.CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump({"main_top_n": 12, "detail_top_n": 34}, f)

            cfg = config_mod.ensure_config()
            with open(config_mod.CONFIG_PATH, "r", encoding="utf-8") as f:
                saved = json.load(f)

        self.assertEqual(cfg["main_top_n"], 12)
        self.assertEqual(cfg["detail_top_n"], 34)
        self.assertEqual(saved["main_top_n"], 12)
        self.assertEqual(saved["detail_top_n"], 34)
        self.assertIn("history_db_path", saved)

    def test_invalid_config_is_not_overwritten_by_ensure_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_mod.CONFIG_PATH = os.path.join(tmp, "config.json")
            with open(config_mod.CONFIG_PATH, "w", encoding="utf-8") as f:
                f.write("{ invalid json")

            with contextlib.redirect_stdout(io.StringIO()):
                cfg = config_mod.ensure_config()
            with open(config_mod.CONFIG_PATH, "r", encoding="utf-8") as f:
                text = f.read()

        self.assertEqual(cfg["main_top_n"], config_mod.DEFAULT_CONFIG["main_top_n"])
        self.assertEqual(text, "{ invalid json")


if __name__ == "__main__":
    unittest.main()
