"""韧性组件库：熔断器、舱壁隔离、超时控制、降级、分位数超时建议。

设计要点（对应演练平台需求 4、5）：
- 熔断器按「错误率」和「慢调用比例」双条件触发，open → half-open → closed 三态；
- half-open 下放行有限个探测调用，成功即关闭、失败即重新打开；
- 舱壁提供信号量版（连接池隔离）和线程池版（线程池隔离）两种；
- 超时在共享守护线程池里执行调用，主线程等待，超时抛 TimeoutError；
- 任意环节失败都可走 fallback 降级；
- Histogram 保留近期延迟样本，给出 p50/p90/p95/p99 分位数建议。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Deque


class CircuitOpenError(RuntimeError):
    """熔断器打开，调用被快速拒绝。"""


class BulkheadRejectedError(RuntimeError):
    """舱壁满员（线程池/连接池占满），调用被拒绝，保护本服务资源。"""


class CallTimeoutError(TimeoutError):
    """调用超时。"""


# ---------------------------------------------------------------- 分位数
class Histogram:
    """固定容量的延迟样本直方图（毫秒），用于分位数超时建议。"""

    def __init__(self, capacity: int = 2000):
        self._samples: Deque[float] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def observe(self, value_ms: float) -> None:
        with self._lock:
            self._samples.append(max(0.0, value_ms))

    def percentile(self, q: float) -> float:
        """q ∈ (0,1]，如 p95 → percentile(0.95)。无样本返回 0。"""
        with self._lock:
            if not self._samples:
                return 0.0
            data = sorted(self._samples)
        if len(data) == 1:
            return data[0]
        idx = min(len(data) - 1, int(round(q * (len(data) - 1))))
        return data[idx]

    def suggest_timeout(self, q: float = 0.95, headroom: float = 1.5) -> float:
        """按历史分位数给超时建议：p(q) × 余量系数，下限 10ms。"""
        p = self.percentile(q)
        if p <= 0:
            return 0.0
        return max(10.0, round(p * headroom, 1))

    def count(self) -> int:
        with self._lock:
            return len(self._samples)


# ---------------------------------------------------------------- 熔断器
@dataclass
class BreakerConfig:
    name: str = "breaker"
    window_s: float = 10.0            # 滚动统计窗口
    min_calls: int = 10               # 窗口内至少多少调用才参与判定
    error_rate_threshold: float = 0.5
    slow_rate_threshold: float = 0.5
    slow_call_ms: float = 800.0       # 超过该耗时记为慢调用
    open_hold_s: float = 5.0          # open 持续多久后进入 half-open
    half_open_max_calls: int = 3      # half-open 探测调用上限
    half_open_success_required: int = 2  # 连续/累计成功多少个后关闭


@dataclass
class BreakerSnapshot:
    state: str
    window_calls: int
    error_rate: float
    slow_rate: float
    total_calls: int
    total_rejected: int
    opened_at: float = 0.0


class CircuitBreaker:
    """三态熔断器，所有状态切换都有事件回调，方便报告还原时间线。"""

    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half-open"

    def __init__(self, config: BreakerConfig, on_event: Callable[[str, dict], None] | None = None):
        self.cfg = config
        self.state = self.CLOSED
        # 滚动窗口元素: (timestamp, is_error, is_slow)
        self._window: Deque[tuple[float, bool, bool]] = deque()
        self._lock = threading.RLock()
        self._opened_at = 0.0
        self._half_open_inflight = 0
        self._half_open_success = 0
        self.total_calls = 0
        self.total_rejected = 0
        self.on_event = on_event or (lambda evt, payload: None)

    # ------------------------------------------------ 统计与状态
    def _evict(self, now: float) -> None:
        cutoff = now - self.cfg.window_s
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()

    def _rates(self) -> tuple[int, float, float]:
        calls = len(self._window)
        if calls == 0:
            return 0, 0.0, 0.0
        errors = sum(1 for _, e, _ in self._window if e)
        slows = sum(1 for _, _, s in self._window if s)
        return calls, errors / calls, slows / calls

    def _maybe_trip(self, now: float) -> None:
        """closed 状态下根据错误率/慢调用比例判断是否熔断。"""
        self._evict(now)
        calls, err_rate, slow_rate = self._rates()
        if calls < self.cfg.min_calls:
            return
        if err_rate >= self.cfg.error_rate_threshold or slow_rate >= self.cfg.slow_rate_threshold:
            self.state = self.OPEN
            self._opened_at = now
            self.on_event("breaker_opened", {
                "breaker": self.cfg.name, "error_rate": round(err_rate, 3),
                "slow_rate": round(slow_rate, 3), "at": now})

    def _enter_half_open_if_due(self, now: float) -> None:
        if self.state == self.OPEN and now - self._opened_at >= self.cfg.open_hold_s:
            self.state = self.HALF_OPEN
            self._half_open_inflight = 0
            self._half_open_success = 0
            self.on_event("breaker_half_open", {"breaker": self.cfg.name, "at": now})

    def snapshot(self) -> BreakerSnapshot:
        with self._lock:
            self._evict(time.monotonic())
            calls, err, slow = self._rates()
            return BreakerSnapshot(
                state=self.state, window_calls=calls, error_rate=round(err, 3),
                slow_rate=round(slow, 3), total_calls=self.total_calls,
                total_rejected=self.total_rejected, opened_at=self._opened_at)

    # ------------------------------------------------ 执行入口
    def allow(self) -> bool:
        """申请调用许可；被熔断拒绝返回 False。"""
        now = time.monotonic()
        with self._lock:
            self._enter_half_open_if_due(now)
            if self.state == self.OPEN:
                self.total_rejected += 1
                return False
            if self.state == self.HALF_OPEN:
                if self._half_open_inflight >= self.cfg.half_open_max_calls:
                    self.total_rejected += 1
                    return False
                self._half_open_inflight += 1
                return True
            self.total_calls += 1
            return True

    def record(self, is_error: bool, is_slow: bool) -> None:
        """上报一次调用结果（耗时由调用方判定慢不慢）。"""
        now = time.monotonic()
        with self._lock:
            if self.state == self.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                if is_error:
                    # 探测失败 → 重新打开，重新计时
                    self.state = self.OPEN
                    self._opened_at = now
                    self.on_event("breaker_reopened", {"breaker": self.cfg.name, "at": now})
                else:
                    self._half_open_success += 1
                    if self._half_open_success >= self.cfg.half_open_success_required:
                        self.state = self.CLOSED
                        self._window.clear()
                        self.on_event("breaker_closed", {"breaker": self.cfg.name, "at": now})
                return
            self._window.append((now, is_error, is_slow))
            if self.state == self.CLOSED:
                self._maybe_trip(now)

    def call(self, func: Callable[[], Any]) -> Any:
        """便捷封装：许可 + 执行 + 结果上报，异常向上抛。"""
        if not self.allow():
            raise CircuitOpenError(f"熔断器 {self.cfg.name} 打开，请求被快速拒绝")
        start = time.monotonic()
        try:
            result = func()
        except BaseException:
            self.record(True, False)
            raise
        elapsed_ms = (time.monotonic() - start) * 1000.0
        self.record(False, elapsed_ms >= self.cfg.slow_call_ms)
        return result

    def reset(self) -> None:
        with self._lock:
            self.state = self.CLOSED
            self._window.clear()
            self._half_open_inflight = self._half_open_success = 0


# ---------------------------------------------------------------- 舱壁
class SemaphoreBulkhead:
    """信号量舱壁：模拟「每个下游一个连接池」，占满即快速失败。

    与有界阻塞队列不同，这里满员直接拒绝（fail-fast），
    正是隔离的意义——不让一个慢下游占光本服务所有线程。
    """

    def __init__(self, max_concurrent: int, name: str = "bulkhead",
                 on_event: Callable[[str, dict], None] | None = None):
        self.sem = threading.Semaphore(max_concurrent)
        self.max_concurrent = max_concurrent
        self.name = name
        self.active = 0
        self._lock = threading.Lock()
        self.rejected_total = 0
        self.on_event = on_event or (lambda evt, payload: None)

    def acquire(self, blocking: bool = True, timeout: float | None = None) -> None:
        ok = self.sem.acquire(blocking=blocking, timeout=timeout)
        if not ok:
            self.rejected_total += 1
            self.on_event("bulkhead_rejected", {"bulkhead": self.name, "at": time.monotonic()})
            raise BulkheadRejectedError(f"舱壁 {self.name} 占满（上限 {self.max_concurrent}）")
        with self._lock:
            self.active += 1

    def release(self) -> None:
        with self._lock:
            self.active = max(0, self.active - 1)
        self.sem.release()

    def saturation(self) -> float:
        with self._lock:
            return self.active / self.max_concurrent if self.max_concurrent else 0.0


class ThreadPoolBulkhead:
    """线程池舱壁：调用被隔离在独立的有界线程池中执行，队列满则拒绝。"""

    def __init__(self, max_workers: int, max_queue: int, name: str = "pool-bulkhead",
                 on_event: Callable[[str, dict], None] | None = None):
        self.pool = DaemonThreadPoolExecutor(max_workers=max_workers,
                                            thread_name_prefix=f"bh-{name}")
        self.sem = threading.Semaphore(max_workers + max_queue)
        self.max_workers = max_workers
        self.max_queue = max_queue
        self.name = name
        self.active = 0
        self._lock = threading.Lock()
        self.rejected_total = 0
        self.on_event = on_event or (lambda evt, payload: None)

    def submit(self, func: Callable[[], Any]):
        if not self.sem.acquire(blocking=False):
            self.rejected_total += 1
            self.on_event("bulkhead_rejected", {"bulkhead": self.name, "at": time.monotonic()})
            raise BulkheadRejectedError(f"线程池舱壁 {self.name} 队列满")
        with self._lock:
            self.active += 1

        def wrapped():
            try:
                return func()
            finally:
                with self._lock:
                    self.active = max(0, self.active - 1)
                self.sem.release()

        return self.pool.submit(wrapped)

    def saturation(self) -> float:
        with self._lock:
            cap = self.max_workers + self.max_queue
            return self.active / cap if cap else 0.0


# ---------------------------------------------------------------- 超时
import weakref as _weakref
from concurrent.futures import _base as _cf_base
from concurrent.futures import thread as _cf_thread


class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    """工作线程全部为守护线程的执行器：空闲线程不阻止进程退出。"""

    def _adjust_thread_count(self) -> None:
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = f"{self._thread_name_prefix or self}_{num_threads}"
            t = threading.Thread(
                name=thread_name, target=_cf_thread._worker,
                args=(_weakref.ref(self, weakref_cb), self._work_queue,
                      self._initializer, self._initargs),
                daemon=True)
            t.start()
            self._threads.add(t)
            _cf_thread._threads_queues[t] = self._work_queue


_shared_timer_pool = DaemonThreadPoolExecutor(
    max_workers=128, thread_name_prefix="chaos-timeout")


def shutdown_shared_pools() -> None:
    """测试/进程收尾时显式关闭共享超时执行器（cancel_futures 取消排队任务）。"""
    _shared_timer_pool.shutdown(wait=False, cancel_futures=True)


def call_with_timeout(func: Callable[[], Any], timeout_s: float) -> Any:
    """带超时执行。超时后后台线程可能仍在跑（Python 无法强杀线程），
    但主线程立即拿到 CallTimeoutError 释放自身工作线程——
    这正是防止线程被慢下游无限期占住的关键。"""
    if timeout_s <= 0:
        return func()
    fut = _shared_timer_pool.submit(func)
    try:
        return fut.result(timeout=timeout_s)
    except TimeoutError as exc:  # Future.result 超时报这个
        raise CallTimeoutError(f"调用超过 {timeout_s}s 超时") from exc


# ---------------------------------------------------------------- 一体化保护调用
@dataclass
class ResiliencePolicy:
    """一个下游依赖的完整韧性策略。mode='off' 时全部失效（对照实验用）。"""

    name: str
    mode: str = "on"                          # on / off
    timeout_s: float = 2.0
    breaker: CircuitBreaker | None = None
    bulkhead: SemaphoreBulkhead | None = None
    fallback: Callable[[Exception], Any] | None = None
    histogram: Histogram = field(default_factory=Histogram)

    def execute(self, func: Callable[[], Any]) -> Any:
        """按 舱壁 → 熔断 → 超时 → 执行 → 上报 → 降级 的顺序保护一次调用。"""
        if self.mode == "off":
            # 对照模式：裸调，无超时无隔离无熔断，故障原样向上传播
            return func()

        try:
            if self.bulkhead is not None:
                # 非阻塞：池满立即拒绝走降级，绝不能排队等下游——
                # 否则慢下游会把本服务线程一个个挂在等待队列上
                self.bulkhead.acquire(blocking=False)
            try:
                if self.breaker is not None and not self.breaker.allow():
                    raise CircuitOpenError(f"熔断器 {self.name} 打开，快速拒绝")
                start = time.monotonic()
                try:
                    result = call_with_timeout(func, self.timeout_s)
                except CallTimeoutError:
                    elapsed_ms = self.timeout_s * 1000.0
                    if self.breaker is not None:
                        self.breaker.record(True, True)
                    self.histogram.observe(elapsed_ms)
                    raise
                elapsed_ms = (time.monotonic() - start) * 1000.0
                self.histogram.observe(elapsed_ms)
                if self.breaker is not None:
                    self.breaker.record(
                        False, elapsed_ms >= self.breaker.cfg.slow_call_ms)
                return result
            finally:
                if self.bulkhead is not None:
                    self.bulkhead.release()
        except Exception as exc:
            if self.fallback is not None:
                return self.fallback(exc)
            raise


# ---------------------------------------------------------------- 超时建议
def advise_timeouts(histograms: dict[str, Histogram], q: float = 0.95,
                    headroom: float = 1.5) -> dict[str, float]:
    """需求 5：根据各下游历史延迟分位数，批量给出超时建议（秒）。"""
    out: dict[str, float] = {}
    for name, hist in histograms.items():
        ms = hist.suggest_timeout(q, headroom)
        if ms > 0:
            out[name] = round(ms / 1000.0, 3)
    return out
