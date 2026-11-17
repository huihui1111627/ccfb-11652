"""存储层与数据模型序列化测试。"""
import os
import tempfile
import unittest

from chaoslab.model import (
    DrillPlan, FaultRule, PlanStep, SteadyStateHypothesis, RunRecord,
    plan_from_dict, to_jsonable,
)
from chaoslab.store import Store


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "t.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _plan(self):
        return DrillPlan(
            name="测试计划", duration_s=30, resilience_modes=["off", "on"],
            steps=[PlanStep(offset_s=1, action="inject",
                            rule=FaultRule(name="r", target="sms",
                                           fault_type="delay", delay_ms=100)),
                   PlanStep(offset_s=10, action="retract", rule_name="r")],
            steady_states=[SteadyStateHypothesis(
                name="成功率", metric="success_rate", threshold=0.9)])

    def test_plan_roundtrip(self):
        plan = self._plan()
        self.store.save_plan_template(plan)
        loaded = self.store.load_plan_template("测试计划")
        self.assertEqual(to_jsonable(loaded), to_jsonable(plan))
        self.assertEqual(loaded.sorted_steps()[0].offset_s, 1)
        self.assertEqual(loaded.steps[0].rule.delay_ms, 100)

    def test_run_and_series(self):
        run = RunRecord(plan_name="测试计划", resilience_mode="on")
        self.store.save_run(run)
        self.store.append_metric(run.run_id, 1.0, "gw-1",
                                 {"thread_pool": {"saturation": 0.5}})
        got = self.store.get_run(run.run_id)
        self.assertEqual(got["resilience_mode"], "on")
        series = self.store.get_series(run.run_id)
        self.assertEqual(series[0]["thread_pool"]["saturation"], 0.5)
        runs = self.store.latest_two_runs("测试计划")
        self.assertEqual(len(runs), 1)

    def test_list_and_delete(self):
        self.store.save_plan_template(self._plan())
        self.assertEqual(len(self.store.list_plan_templates()), 1)
        self.assertTrue(self.store.delete_plan_template("测试计划"))
        self.assertIsNone(self.store.load_plan_template("测试计划"))


if __name__ == "__main__":
    unittest.main()
