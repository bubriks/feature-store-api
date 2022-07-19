#
#   Copyright 2021 Logical Clocks AB
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hsfs import client
from hsfs import engine
from hsfs.core.storage_connector_api import StorageConnectorApi
from hsfs.storage_connector import (
    StorageConnector,
    HopsFSConnector,
    S3Connector,
    JdbcConnector,
    RedshiftConnector,
    AdlsConnector,
    SnowflakeConnector,
    KafkaConnector,
    GcsConnector,
    BigQueryConnector,
)


class TestHopsFSConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "HOPSFS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.HOPSFS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = HopsFSConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "HOPSFS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.HOPSFS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = HopsFSConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "HOPSFS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.HOPSFS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_always_empty(self):
        # Arrange
        sc = HopsFSConnector(
            id=1,
            name="sc",
            featurestore_id="99",
            description="test",
            hopsfs_path="test",
            dataset_name="test",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual({}, spark_options)


class TestS3Connector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "S3",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.S3, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = S3Connector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "S3",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.S3, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = S3Connector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "S3",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.S3, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_always_empty(self):
        # Arrange
        sc = S3Connector(
            id=1,
            name="sc",
            featurestore_id="99",
            description="test",
            access_key="test",
            secret_key="test",
            server_encryption_algorithm="test",
            server_encryption_key="test",
            bucket="test",
            session_token="test",
            iam_role="test",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual({}, spark_options)


class TestJdbcConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "JDBC",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.JDBC, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = JdbcConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "JDBC",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.JDBC, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = JdbcConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "JDBC",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.JDBC, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_arguments_none(self):
        # Arrange
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=None,
        )

        # Act
        spark_options = jdbc_connector.spark_options()

        # Assert
        self.assertEqual(connection_string, spark_options["url"])

    def test_spark_options_arguments_empty(self):
        # Arrange
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments="",
        )

        # Act
        spark_options = jdbc_connector.spark_options()

        # Assert
        self.assertEqual(connection_string, spark_options["url"])

    def test_spark_options_arguments_arguments(self):
        # Arrange
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )
        arguments = [
            {"name": "arg1", "value": "value1"},
            {"name": "arg2", "value": "value2"},
        ]

        jdbc_connector = JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=arguments,
        )

        # Act
        spark_options = jdbc_connector.spark_options()

        # Assert
        self.assertEqual(connection_string, spark_options["url"])
        self.assertEqual("value1", spark_options["arg1"])
        self.assertEqual("value2", spark_options["arg2"])


class TestRedshiftConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "REDSHIFT",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.REDSHIFT, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = RedshiftConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "REDSHIFT",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.REDSHIFT, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = RedshiftConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "REDSHIFT",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.REDSHIFT, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_table_name_none(self):
        # Arrange
        sc = RedshiftConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            description="",
            cluster_identifier="",
            database_driver="",
            database_endpoint="",
            database_name="",
            database_port="",
            table_name=None,
            database_user_name="",
            auto_create="",
            database_password="",
            database_group="",
            iam_role="",
            arguments="",
            expiration="",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("dbtable", spark_options)

    def test_spark_options_table_name_empty(self):
        # Arrange
        sc = RedshiftConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            description="",
            cluster_identifier="",
            database_driver="",
            database_endpoint="",
            database_name="",
            database_port="",
            table_name="",
            database_user_name="",
            auto_create="",
            database_password="",
            database_group="",
            iam_role="",
            arguments="",
            expiration="",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("dbtable", spark_options)
        self.assertEqual("", spark_options["dbtable"])

    def test_spark_options_table_name_value(self):
        # Arrange
        sc = RedshiftConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            description="",
            cluster_identifier="",
            database_driver="",
            database_endpoint="",
            database_name="",
            database_port="",
            table_name="test",
            database_user_name="",
            auto_create="",
            database_password="",
            database_group="",
            iam_role="",
            arguments="",
            expiration="",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("dbtable", spark_options)
        self.assertEqual("test", spark_options["dbtable"])


class TestAdlsConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "ADLS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.ADLS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = AdlsConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "ADLS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.ADLS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = AdlsConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "ADLS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.ADLS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_none(self):
        # Arrange
        sc = AdlsConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            generation="test",
            directory_id="test",
            application_id="test",
            service_credential="test",
            account_name="test",
            container_name="test",
            spark_options=None,
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual({}, spark_options)

    def test_spark_options_empty(self):
        # Arrange
        sc = AdlsConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            generation="test",
            directory_id="test",
            application_id="test",
            service_credential="test",
            account_name="test",
            container_name="test",
            spark_options={},
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual({}, spark_options)

    def test_spark_options_value(self):
        # Arrange
        sc = AdlsConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            generation="test",
            directory_id="test",
            application_id="test",
            service_credential="test",
            account_name="test",
            container_name="test",
            spark_options=[{"name": "test", "value": 1}],
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual(1, spark_options["test"])


class TestSnowflakeConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "SNOWFLAKE",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.SNOWFLAKE, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = SnowflakeConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "SNOWFLAKE",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.SNOWFLAKE, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = SnowflakeConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "SNOWFLAKE",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.SNOWFLAKE, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_db_table_none(self):
        # Arrange
        sc = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=None
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("dbtable", spark_options)

    def test_spark_options_db_table_empty(self):
        # Arrange
        sc = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=""
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("dbtable", spark_options)

    def test_spark_options_db_table_value(self):
        # Arrange
        sc = SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="test"
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("dbtable", spark_options)
        self.assertEqual("test", spark_options["dbtable"])


class TestKafkaConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

            # Initializes engine
            engine.init("training")

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "KAFKA",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.KAFKA, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = KafkaConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "KAFKA",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.KAFKA, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = KafkaConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "KAFKA",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.KAFKA, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_bootstrap_servers_none(self):
        # Arrange
        sc = KafkaConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            bootstrap_servers=None,
            security_protocol="test",
            ssl_truststore_location="test",
            ssl_truststore_password="test",
            ssl_keystore_location="test",
            ssl_keystore_password="test",
            ssl_key_password="test",
            ssl_endpoint_identification_algorithm="test",
            options=None,
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("kafka.bootstrap.servers", spark_options)

    def test_spark_options_db_table_empty(self):
        # Arrange
        sc = KafkaConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            bootstrap_servers="",
            security_protocol="test",
            ssl_truststore_location="test",
            ssl_truststore_password="test",
            ssl_keystore_location="test",
            ssl_keystore_password="test",
            ssl_key_password="test",
            ssl_endpoint_identification_algorithm="test",
            options=None,
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("kafka.bootstrap.servers", spark_options)
        self.assertEqual("", spark_options["kafka.bootstrap.servers"])

    def test_spark_options_db_table_value(self):
        # Arrange
        sc = KafkaConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            bootstrap_servers="test",
            security_protocol="test",
            ssl_truststore_location="test",
            ssl_truststore_password="test",
            ssl_keystore_location="test",
            ssl_keystore_password="test",
            ssl_key_password="test",
            ssl_endpoint_identification_algorithm="test",
            options=None,
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("kafka.bootstrap.servers", spark_options)
        self.assertEqual("test", spark_options["kafka.bootstrap.servers"])


# todo GCS must have description while others dont
class TestGcsConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "GCS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "test",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.GCS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = GcsConnector(1, "sc", "test", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "GCS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.GCS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = GcsConnector(1, "sc", "test", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "GCS",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "test",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.GCS, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_empty(self):
        # Arrange
        sc = GcsConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            key_path="test",
            bucket="test",
            algorithm="test",
            encryption_key="test",
            encryption_key_hash="test",
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertEqual({}, spark_options)


class TestBigQueryConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API

            # Configure the mock return value
            mock_send.return_value.status_code = 200
            mock_send.return_value.content = "project_id"

            # Initializes client
            client.init(
                "external",
                host="1",
                port=1,
                project="2",
                region_name="3-a",
                api_key_value="4",
            )

    def test_get_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "BIGQUERY",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get("sc")

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.BIGQUERY, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get("sc")
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_refetch_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = BigQueryConnector(1, "sc", 99)
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "BIGQUERY",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "new",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.refetch(sc)

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.BIGQUERY, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)
            self.assertEqual("new", response.description)
            self.assertEqual(sc, response)

    def test_refetch_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            sc = BigQueryConnector(1, "sc", 99)
            mock_send.return_value.status_code = 400

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.refetch(sc)
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)
            self.assertNotEqual(sc, response)

    def test_get_online_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 200
            json = {
                "type": "",
                "storage_connector_type": "BIGQUERY",
                "id": 1,
                "name": "sc",
                "featurestore_id": 99,
                "description": "test",
            }
            mock_send.return_value.content = str(
                json
            )  # content can't be None to extract json
            mock_send.return_value.json = Mock(return_value=json)

            storageConnectorApi = StorageConnectorApi(1)

            # Act
            response = storageConnectorApi.get_online_connector()

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(StorageConnector.BIGQUERY, response.type)
            self.assertEqual(1, response.id)
            self.assertEqual("sc", response.name)
            self.assertEqual(99, response._featurestore_id)

    def test_get_online_none_existing_connector(self):
        with patch.object(
            Session, "send"
        ) as mock_send:  # Session class patched to avoid communication to API
            # Arrange
            mock_send.return_value.status_code = 400
            storageConnectorApi = StorageConnectorApi(1)

            # Act
            try:
                response = storageConnectorApi.get_online_connector()
            except:
                response = None
                pass

            # Assert
            self.assertEqual(1, mock_send.call_count)
            self.assertEqual(None, response)

    def test_spark_options_dataset_none(self):
        # Arrange
        sc = BigQueryConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            key_path="test",
            parent_project="test",
            dataset=None,
            query_table="test",
            query_project="test",
            materialization_dataset="test",
            arguments={},
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("dataset", spark_options)

    def test_spark_options_dataset_empty(self):
        # Arrange
        sc = BigQueryConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            key_path="test",
            parent_project="test",
            dataset="",
            query_table="test",
            query_project="test",
            materialization_dataset="test",
            arguments={},
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertNotIn("dataset", spark_options)

    def test_spark_options_dataset_value(self):
        # Arrange
        sc = BigQueryConnector(
            id=1,
            name="sc",
            featurestore_id=99,
            description="test",
            key_path="test",
            parent_project="test",
            dataset="test",
            query_table="test",
            query_project="test",
            materialization_dataset="test",
            arguments={},
        )

        # Act
        spark_options = sc.spark_options()

        # Assert
        self.assertIn("dataset", spark_options)
        self.assertEqual("test", spark_options["dataset"])
