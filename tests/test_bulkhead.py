"""舱壁、超时、降级、分位数建议测试。"""
import unittest

from chaoslab.resilience import (
    BulkheadRejectedError, CallTimeoutError, Histogram, ResiliencePolicy,
    SemaphoreBulkhead, call_with_timeout,
)


class BulkheadTest(unittest.TestCase):
    def test_rejects_when_full(self):
        wall = SemaphoreBulkhead(2, name="t")
        wall.acquire()
        wall.acquire()
        self.assertAlmostEqual(wall.saturation(), 1.0)
        with self.assertRaises(BulkheadRejectedError):
            wall.acquire(blocking=False)
        wall.release()
        wall.acquire(blocking=False)  # 释放后可再进

    def test_timeout_frees_main_thread(self):
        import time
        start = time.monotonic()
        with self.assertRaises(CallTimeoutError):
            call_with_timeout(lambda: time.sleep(2), timeout_s=0.2)
        self.assertLess(time.monotonic() - start, 0.5)


class HistogramTest(unittest.TestCase):
    def test_percentile_and_suggestion(self):
        h = Histogram()
        for v in range(1, 101):           # 1ms..100ms
            h.observe(float(v))
        self.assertAlmostEqual(h.percentile(0.95), 95.05, places=1)
        suggested = h.suggest_timeout(0.95, headroom=1.5)
        self.assertGreater(suggested, h.percentile(0.95))
        # 无样本时给 0，不拍脑袋
        self.assertEqual(Histogram().suggest_timeout(), 0.0)


class ResiliencePolicyTest(unittest.TestCase):
    def _policy(self, mode="on", fallback=None):
        wall = SemaphoreBulkhead(3, name="dep")
        return ResiliencePolicy(name="dep", mode=mode, timeout_s=0.5,
                                bulkhead=wall, fallback=fallback)

    def test_off_mode_bare_call(self):
        p = self._policy(mode="off")
        self.assertEqual(p.execute(lambda: 42), 42)

    def test_on_mode_timeout_then_fallback(self):
        import time
        p = self._policy(fallback=lambda exc: {"degraded": True,
                                               "reason": type(exc).__name__})
        result = p.execute(lambda: time.sleep(2))
        self.assertTrue(result["degraded"])
        self.assertEqual(result["reason"], "CallTimeoutError")

    def test_on_mode_success_records_histogram(self):
        p = self._policy()
        p.execute(lambda: "ok")
        self.assertGreater(p.histogram.count(), 0)

    def test_bulkhead_rejection_uses_fallback(self):
        p = self._policy(fallback=lambda exc: {"bulk": True})
        # 占满舱壁
        for _ in range(3):
            p.bulkhead.acquire()
        result = p.execute(lambda: "never")
        self.assertTrue(result["bulk"])


if __name__ == "__main__":
    unittest.main()
