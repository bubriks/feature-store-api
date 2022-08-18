#
#   Copyright 2022 Hopsworks AB
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


from hsfs import feature


class TestFeature:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature"]["get"]["response"]

        # Act
        f = feature.Feature.from_response_json(json)

        # Assert
        assert f.name == "intt"
        assert f.type == "int"
        assert f.description == "test_description"
        assert f.primary == True
        assert f.partition == False
        assert f.hudi_precombine_key == True
        assert f.online_type == "int"
        assert f.default_value == 1
        assert f._feature_group_id == 15

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature"]["get_basic_info"]["response"]

        # Act
        f = feature.Feature.from_response_json(json)

        # Assert
        assert f.name == "intt"
        assert f.type == None
        assert f.description == None
        assert f.primary == False
        assert f.partition == False
        assert f.hudi_precombine_key == False
        assert f.online_type == None
        assert f.default_value == None
        assert f._feature_group_id == None
