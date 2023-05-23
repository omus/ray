from ray_release.test import Test
from ray_release.result import (
    Result,
    ResultStatus,
)
from ray_release.test_state.state_machine import TestStateMachine



def test_move():
    test = Test()
    test.add_test_result(Result(status=ResultStatus.PASSING))
    assert test
    test.add_test_result({"status": "passing"})