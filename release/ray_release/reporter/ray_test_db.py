import json

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.test import Test
from ray_release.test_state.state_machine import TestStateMachine
from ray_release.logger import logger


class RayTestDBReporter(Reporter):
    def report_result(self, test: Test, result: Result) -> None:
        logger.info(
            f"Updating test object {test.get_name()} with result {result.status}"
        )
        test.update_from_s3()
        logger.info(f"Test object before updating: {json.dumps(test)}")
        test.add_test_result(result)
        # Compute and update the next test state
        TestStateMachine(test).move()
        logger.info(f"Test object after updating: {json.dumps(test)}")
        test.persist_to_s3()
        logger.info(f"Test object {test.get_name()} updated successfully")
