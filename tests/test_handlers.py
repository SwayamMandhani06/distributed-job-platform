"""
Unit tests for deterministic job handlers in worker/handlers.py.

Verifies:
  1. math_prime_sieve: Output correctness, edge cases, error handling.
  2. hash_checksum: Correct SHA-256, SHA-512, MD5 hashes, error handling.
  3. text_frequency: Tokenization, top-N frequencies, error handling.
  4. Backward-compatible legacy handlers (email, csv_processing, thumbnail).
  5. dispatch_task dispatcher routing and unknown job_type rejection.
"""

import unittest
from worker.handlers import (
    dispatch_task,
    handle_prime_sieve,
    handle_hash_checksum,
    handle_text_frequency,
    handle_email,
    handle_csv_processing,
    handle_thumbnail,
    MAX_PRIME_SIEVE_LIMIT,
)


class TestPrimeSieveHandler(unittest.TestCase):
    """Test suite for math_prime_sieve handler."""

    def test_prime_sieve_basic_correctness(self):
        # Primes <= 10: 2, 3, 5, 7
        res = handle_prime_sieve({"limit": 10})
        self.assertEqual(res["total_primes"], 4)
        self.assertEqual(res["largest_prime"], 7)
        self.assertEqual(res["sample_primes"], [2, 3, 5, 7])
        self.assertEqual(res["algorithm"], "sieve_of_eratosthenes")

    def test_prime_sieve_limit_100(self):
        # Exactly 25 primes <= 100, largest is 97
        res = handle_prime_sieve({"limit": 100})
        self.assertEqual(res["total_primes"], 25)
        self.assertEqual(res["largest_prime"], 97)

    def test_prime_sieve_alternate_key_n(self):
        res = handle_prime_sieve({"n": 20})
        # Primes <= 20: 2, 3, 5, 7, 11, 13, 17, 19 (8 total)
        self.assertEqual(res["total_primes"], 8)
        self.assertEqual(res["largest_prime"], 19)

    def test_prime_sieve_minimum_valid_bound(self):
        # Limit 2 has exactly 1 prime: 2
        res = handle_prime_sieve({"limit": 2})
        self.assertEqual(res["total_primes"], 1)
        self.assertEqual(res["largest_prime"], 2)

    def test_prime_sieve_invalid_bounds(self):
        with self.assertRaises(ValueError):
            handle_prime_sieve({"limit": 1})
        with self.assertRaises(ValueError):
            handle_prime_sieve({"limit": -5})
        with self.assertRaises(ValueError):
            handle_prime_sieve({"limit": "not_an_int"})
        with self.assertRaises(ValueError):
            handle_prime_sieve({"limit": MAX_PRIME_SIEVE_LIMIT + 1})


class TestHashChecksumHandler(unittest.TestCase):
    """Test suite for hash_checksum handler."""

    def test_sha256_known_digest(self):
        # SHA-256 for "hello world" is b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        res = handle_hash_checksum({"data": "hello world", "algorithm": "sha256"})
        self.assertEqual(res["algorithm"], "sha256")
        self.assertEqual(res["checksum"], "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        self.assertEqual(res["input_bytes"], 11)
        self.assertEqual(res["input_characters"], 11)

    def test_sha512_digest(self):
        res = handle_hash_checksum({"data": "test payload", "algorithm": "sha512"})
        self.assertEqual(res["algorithm"], "sha512")
        self.assertEqual(len(res["checksum"]), 128)  # 128 hex chars for SHA-512

    def test_md5_digest(self):
        res = handle_hash_checksum({"text": "test md5", "algorithm": "md5"})
        self.assertEqual(res["algorithm"], "md5")
        self.assertEqual(len(res["checksum"]), 32)  # 32 hex chars for MD5

    def test_missing_data_payload(self):
        with self.assertRaises(ValueError):
            handle_hash_checksum({})

    def test_unsupported_algorithm(self):
        with self.assertRaises(ValueError):
            handle_hash_checksum({"data": "sample", "algorithm": "sha1_deprecated"})


class TestTextFrequencyHandler(unittest.TestCase):
    """Test suite for text_frequency handler."""

    def test_text_frequency_word_counts(self):
        text = "distributed systems and distributed computing systems"
        res = handle_text_frequency({"text": text, "top_n": 3})
        self.assertEqual(res["total_words"], 6)
        self.assertEqual(res["unique_words"], 4)  # distributed, systems, and, computing
        # distributed: 2, systems: 2
        top_words = {item["word"]: item["count"] for item in res["top_frequencies"]}
        self.assertEqual(top_words["distributed"], 2)
        self.assertEqual(top_words["systems"], 2)

    def test_case_sensitivity(self):
        text = "Python python PYTHON"
        res_ignore = handle_text_frequency({"text": text, "ignore_case": True})
        self.assertEqual(res_ignore["unique_words"], 1)
        self.assertEqual(res_ignore["top_frequencies"][0]["count"], 3)

        res_sensitive = handle_text_frequency({"text": text, "ignore_case": False})
        self.assertEqual(res_sensitive["unique_words"], 3)

    def test_missing_text_payload(self):
        with self.assertRaises(ValueError):
            handle_text_frequency({})

    def test_invalid_top_n(self):
        with self.assertRaises(ValueError):
            handle_text_frequency({"text": "valid text", "top_n": 0})
        with self.assertRaises(ValueError):
            handle_text_frequency({"text": "valid text", "top_n": 500})


class TestBackwardCompatibleHandlers(unittest.TestCase):
    """Test suite for legacy job types."""

    def test_email_handler_valid(self):
        res = handle_email({"to": "alice@example.com", "subject": "Test", "body": "Body content"})
        self.assertEqual(res["status"], "delivered")
        self.assertEqual(res["recipient"], "alice@example.com")
        self.assertTrue(res["message_id"].startswith("msg-"))

    def test_email_handler_invalid_recipient(self):
        with self.assertRaises(ValueError):
            handle_email({"to": "invalid-email-no-at", "subject": "Test"})

    def test_csv_processing_handler(self):
        res = handle_csv_processing({"file_path": "/data/test.csv", "operations": ["sort", "filter"]})
        self.assertEqual(res["file_path"], "/data/test.csv")
        self.assertEqual(res["operations_applied"], ["sort", "filter"])
        self.assertEqual(res["records_processed"], 1000)

    def test_thumbnail_handler(self):
        res = handle_thumbnail({"image_url": "https://example.com/pic.jpg", "sizes": [100, 200]})
        self.assertEqual(res["total_variants"], 2)
        self.assertEqual(res["variants_generated"][0]["dimensions"], "100x100")


class TestDispatchTask(unittest.TestCase):
    """Test suite for central task dispatcher."""

    def test_dispatch_known_task(self):
        res = dispatch_task("math_prime_sieve", {"limit": 10})
        self.assertEqual(res["total_primes"], 4)

    def test_dispatch_unsupported_task(self):
        with self.assertRaises(ValueError) as ctx:
            dispatch_task("unknown_quantum_job", {})
        self.assertIn("Unsupported job_type 'unknown_quantum_job'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
