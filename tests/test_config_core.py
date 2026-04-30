import asyncio
import unittest

from core.config import human_error, mask_sensitive_text


class ConfigCoreTests(unittest.TestCase):
    def test_human_error_explains_incomplete_read(self):
        exc = asyncio.IncompleteReadError(partial=b"", expected=2)

        text = human_error(exc)

        self.assertIn("соединение оборвалось", text)
        self.assertIn("proxy", text)

    def test_mask_sensitive_text_hides_proxy_credentials(self):
        text = "http://user:password@example.com:1234 failed"

        masked = mask_sensitive_text(text)

        self.assertNotIn("user:password", masked)
        self.assertIn("***:***", masked)


if __name__ == "__main__":
    unittest.main()
