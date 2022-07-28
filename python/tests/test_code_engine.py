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
import pytest

from hsfs import engine, training_dataset, feature_group
from hsfs.core import code_engine, code_api

class TestCodeEngine:

    @pytest.fixture
    def reset(self):
        os.environ.pop("HOPSWORKS_KERNEL_ID", None)
        os.environ.pop("HOPSWORKS_JOB_NAME", None)
        sys.modules.pop("pyspark.dbutils", None)

    def test_td_save_jupyter(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(td)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.JUPYTER

    def test_td_save_job(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(td)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.JOB

    def test_td_save_databricks(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        sys.modules["pyspark.dbutils"] = mocker.Mock()
        sys.modules["pyspark.dbutils"].__spec__ = mocker.Mock()
        json = """{"extraContext": {"notebook_path": "test_path"},
                    "tags": {"browserHostName": "test_browser_host_name"}}"""
        sys.modules[
            "pyspark.dbutils"
        ].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson = mocker.Mock(
            return_value=json
        )

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(td)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.DATABRICKS

    def test_fg_save_jupyter(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        os.environ.setdefault("HOPSWORKS_KERNEL_ID", "1")

        mocker.patch("hsfs.engine.get_type")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(fg)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.JUPYTER

    def test_fg_save_job(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        os.environ.setdefault("HOPSWORKS_JOB_NAME", "1")

        mocker.patch("hsfs.engine.get_type")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(fg)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.JOB

    def test_fg_save_databricks(self, mocker, reset):
        # Arrange
        feature_store_id = 99

        sys.modules["pyspark.dbutils"] = mocker.Mock()
        sys.modules["pyspark.dbutils"].__spec__ = mocker.Mock()
        json = """{"extraContext": {"notebook_path": "test_path"},
                    "tags": {"browserHostName": "test_browser_host_name"}}"""
        sys.modules[
            "pyspark.dbutils"
        ].DBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson = mocker.Mock(
            return_value=json
        )
        mocker.patch("hsfs.engine.get_type")
        mock_code_api_post = mocker.patch("hsfs.core.code_api.CodeApi.post")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=0,
        )
        c_engine = code_engine.CodeEngine(feature_store_id, "trainingdatasets")

        # Act
        c_engine.save_code(fg)

        # Assert
        assert mock_code_api_post.call_count == 1
        assert mock_code_api_post.call_args.kwargs["code_type"] == code_engine.RunType.DATABRICKS