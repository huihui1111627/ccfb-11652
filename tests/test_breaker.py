"""熔断器测试：错误率/慢调用双触发、half-open 探测恢复。"""
import time
import unittest

from chaoslab.resilience import (
    BreakerConfig, CircuitBreaker, CircuitOpenError,
)


class CircuitBreakerTest(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.breaker = CircuitBreaker(BreakerConfig(
            name="t", window_s=30, min_calls=6,
            error_rate_threshold=0.5, slow_rate_threshold=0.5,
            slow_call_ms=100, open_hold_s=0.3, half_open_max_calls=2,
            half_open_success_required=2),
            on_event=lambda evt, p: self.events.append(evt))

    def test_opens_on_error_rate(self):
        # 前 3 个成功，后 3 个失败 → 错误率 50%，达到阈值
        for _ in range(3):
            self.breaker.record(False, False)
        for _ in range(3):
            self.breaker.record(True, False)
        self.assertEqual(self.breaker.state, CircuitBreaker.OPEN)
        self.assertIn("breaker_opened", self.events)
        # 打开后直接拒绝
        self.assertFalse(self.breaker.allow())

    def test_opens_on_slow_call_rate(self):
        # 全部成功但都是慢调用 → 慢调用比例 100%
        for _ in range(6):
            self.breaker.record(False, True)
        self.assertEqual(self.breaker.state, CircuitBreaker.OPEN)

    def test_half_open_recovers(self):
        for _ in range(6):
            self.breaker.record(True, False)
        self.assertEqual(self.breaker.state, CircuitBreaker.OPEN)
        time.sleep(0.35)
        # 进入 half-open，探测调用成功达到要求 → 关闭
        self.assertTrue(self.breaker.allow())
        self.breaker.record(False, False)
        self.assertTrue(self.breaker.allow())
        self.breaker.record(False, False)
        self.assertEqual(self.breaker.state, CircuitBreaker.CLOSED)
        self.assertIn("breaker_half_open", self.events)
        self.assertIn("breaker_closed", self.events)

    def test_half_open_failure_reopens(self):
        for _ in range(6):
            self.breaker.record(True, False)
        time.sleep(0.35)
        self.assertTrue(self.breaker.allow())
        self.breaker.record(True, False)
        self.assertEqual(self.breaker.state, CircuitBreaker.OPEN)
        self.assertIn("breaker_reopened", self.events)

    def test_below_min_calls_no_trip(self):
        for _ in range(3):
            self.breaker.record(True, False)
        self.assertEqual(self.breaker.state, CircuitBreaker.CLOSED)

    def test_call_helper_raises_when_open(self):
        for _ in range(6):
            self.breaker.record(True, False)
        with self.assertRaises(CircuitOpenError):
            self.breaker.call(lambda: 1)

    def test_rolling_window_eviction(self):
        b = CircuitBreaker(BreakerConfig(
            name="w", window_s=0.2, min_calls=4,
            error_rate_threshold=0.5, slow_rate_threshold=0.5))
        for _ in range(4):
            b.record(True, False)
        self.assertEqual(b.state, CircuitBreaker.OPEN)
        b.reset()
        for _ in range(4):
            b.record(True, False)
        self.assertEqual(b.state, CircuitBreaker.OPEN)
        time.sleep(0.25)
        b._evict(time.monotonic())
        self.assertEqual(b._rates()[0], 0)


if __name__ == "__main__":
    unittest.main()
