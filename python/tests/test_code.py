#
#   Copyright 2022 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import os
import sys
import unittest
from requests import Session
from unittest.mock import Mock
from unittest.mock import patch

from python.hsfs import client
from python.hsfs import engine
from python.hsfs.core.code_engine import CodeEngine
from python.hsfs.training_dataset import TrainingDataset
from python.hsfs.feature_group import FeatureGroup

class TestCodeTrainingDataset(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            json = {"projectId": 1}
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            # Initializes client
            client.init(
                client_type="external",
                host="localhost",
                port=88,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_jupyter(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")
            td_entity = TrainingDataset("test",
                                        1,
                                        "CSV",
                                        99,
                                        splits={},
                                        id=0)
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/trainingdatasets/0/code?entityId=1&type=JUPYTER", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)

            os.environ.pop("HOPSWORKS_KERNEL_ID")

    def test_send_job_code(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")
            td_entity = TrainingDataset("test",
                                        1,
                                        "CSV",
                                        99,
                                        splits={},
                                        id=0)
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/trainingdatasets/0/code?entityId=1&type=JOB", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)

            os.environ.pop("HOPSWORKS_JOB_NAME")

    def test_databricks(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            sys.modules['pyspark.dbutils'] = Mock()
            sys.modules['pyspark.dbutils'].__spec__ = Mock()
            sys.modules['pyspark.dbutils'].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson =\
                Mock(return_value='{"extraContext": {"notebook_path": "test_path"}, "tags": {"browserHostName": "test_browser_host_name"}}')
            td_entity = TrainingDataset("test",
                                        1,
                                        "CSV",
                                        99,
                                        splits={},
                                        id=0)
            codeEngine = CodeEngine(99, "trainingdatasets")

            # Act
            codeEngine.save_code(td_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/trainingdatasets/0/code?entityId=test_path&type=DATABRICKS&databricksClusterId=test_browser_host_name", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)

class TestCodeFeatureGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            json = {"projectId": 1}
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            # Initializes client
            client.init(
                client_type="external",
                host="localhost",
                port=88,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

            # Initializes engine
            engine.init("training")


    def test_jupyter(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")
            fg_entity = FeatureGroup("test",
                                        1,
                                        99,
                                     primary_key=[],
                                     partition_key=[],
                                     id=0)
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/featuregroups/0/code?entityId=1&type=JUPYTER", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)

            os.environ.pop("HOPSWORKS_KERNEL_ID")

    def test_send_job_code(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")
            fg_entity = FeatureGroup("test",
                                     1,
                                     99,
                                     primary_key=[],
                                     partition_key=[],
                                     id=0)
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/featuregroups/0/code?entityId=1&type=JOB", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)

            os.environ.pop("HOPSWORKS_JOB_NAME")

    def test_databricks(self):
        with patch.object(
                Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            sys.modules['pyspark.dbutils'] = Mock()
            sys.modules['pyspark.dbutils'].__spec__ = Mock()
            sys.modules['pyspark.dbutils'].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson = \
                Mock(return_value='{"extraContext": {"notebook_path": "test_path"}, "tags": {"browserHostName": "test_browser_host_name"}}')
            fg_entity = FeatureGroup("test",
                                     1,
                                     99,
                                     primary_key=[],
                                     partition_key=[],
                                     id=0)
            codeEngine = CodeEngine(99, "featuregroups")

            # Act
            codeEngine.save_code(fg_entity)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertIn("commitTime", mock_send.call_args.args[0].body)
            self.assertEqual("https://localhost:88/hopsworks-api/api/project/1/featurestores/99/featuregroups/0/code?entityId=test_path&type=DATABRICKS&databricksClusterId=test_browser_host_name", mock_send.call_args.args[0].url)
            self.assertIsNotNone(mock_send.call_args.args[0].headers)