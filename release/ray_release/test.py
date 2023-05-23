import enum
import os
import json
import time
from typing import Optional, List
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

from ray_release.result import (
    ResultStatus,
    Result,
)
from ray_release.logger import logger

AWS_BUCKET = "ray-ci-results"
AWS_KEY = "ray_tests"
DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.7").split(".")
)
DATAPLANE_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DATAPLANE_ECR_REPO = "anyscale/ray"
DATAPLANE_ECR_ML_REPO = "anyscale/ray-ml"


class TestState(enum.Enum):
    """
    Overall state of the test
    """

    JAILED = "jailed"
    FAILING = "failing"
    PASSING = "passing"


@dataclass
class TestResult:
    status: ResultStatus
    timestamp: int

    @classmethod
    def from_result(cls, result: Result):
        return cls(
            status=result.status,
            timestamp=int(time.time() * 1000),
        )

    @classmethod
    def from_dict(cls, result: dict):
        return cls(
            status=result["status"],
            timestamp=result["timestamp"],
        )

    def is_failing(self) -> bool:
        return not self.is_passing()

    def is_passing(self) -> bool:
        return self.status == ResultStatus.SUCCESS


class Test(dict):
    """A class represents a test to run on buildkite"""

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        return self["cluster"].get("byod", False)

    def get_byod_type(self) -> Optional[str]:
        """
        Returns the type of the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return None
        return self["cluster"]["byod"]["type"]

    def get_byod_pre_run_cmds(self) -> List[str]:
        """
        Returns the list of pre-run commands for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return []
        return self["cluster"]["byod"].get("pre_run_cmds", [])

    def get_name(self) -> str:
        """
        Returns the name of the test.
        """
        return self["name"]

    def get_state(self) -> TestState:
        """
        Returns the state of the test.
        """
        return TestState(self.get("state", TestState.GOOD.value))

    def set_state(self, state: TestState) -> None:
        """
        Sets the state of the test.
        """
        self["state"] = state.value

    def get_python_version(self) -> str:
        """
        Returns the python version to use for this test. If not specified, use
        the default python version.
        """
        return self.get("python", ".".join(str(v) for v in DEFAULT_PYTHON_VERSION))

    def get_byod_image_tag(self) -> str:
        """
        Returns the byod image tag to use for this test.
        """
        ray_version = (
            os.environ.get("COMMIT_TO_TEST", "")[:6]
            or os.environ.get("BUILDKITE_COMMIT", "")[:6]
        )
        image_suffix = "-gpu" if self.get_byod_type() == "gpu" else ""
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"{ray_version}-{python_version}{image_suffix}"

    def get_byod_repo(self) -> str:
        """
        Returns the byod repo to use for this test.
        """
        return (
            DATAPLANE_ECR_ML_REPO
            if self.get_byod_type() == "gpu"
            else DATAPLANE_ECR_REPO
        )

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test.
        """
        ray_project = "ray-ml" if self.get_byod_type() == "gpu" else "ray"
        return f"rayproject/{ray_project}:{self.get_byod_image_tag()}"

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return f"{DATAPLANE_ECR}/{self.get_byod_repo()}:{self.get_byod_image_tag()}"

    def update_from_s3(self) -> None:
        """
        Update test object with data field from s3
        """
        try:
            data = (
                boto3.client("s3")
                .get_object(
                    Bucket=AWS_BUCKET,
                    Key=f"{AWS_KEY}/{self.get_name()}.json",
                )
                .get("Body")
                .read()
                .decode("utf-8")
            )
        except ClientError as e:
            logger.warning(f"Failed to update data for {self.get_name()} from s3:  {e}")
            return
        self.update(json.loads(data))

    def add_test_result(self, result: Result, limit: int = 10) -> None:
        """
        Add test result to test object
        """

        def _get_latest_result(test_result: TestResult):
            return test_result.timestamp

        test_results = sorted(
            self.get_test_results() + [TestResult.from_result(result)],
            key=_get_latest_result,
            reverse=True,
        )[:limit]
        self["results"] = [test_result.__dict__ for test_result in test_results]

    def get_test_results(self, sorted: bool = False) -> List[TestResult]:
        """
        Get test result from test object
        """

        def _get_latest_result(test_result: TestResult):
            return test_result.timestamp

        test_results = [
            TestResult.from_dict(result) for result in self.get("results", [])
        ]
        if sorted:
            return sorted(test_results, key=_get_latest_result, reverse=True)
        return test_results

    def persist_to_s3(self) -> bool:
        """
        Persist test object to s3
        """
        boto3.client("s3").put_object(
            Bucket=AWS_BUCKET,
            Key=f"{AWS_KEY}/{self.get_name()}.json",
            Body=json.dumps(self),
        )


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
