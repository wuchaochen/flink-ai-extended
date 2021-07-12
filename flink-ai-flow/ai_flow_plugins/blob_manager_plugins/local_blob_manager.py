#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import tempfile
import zipfile
from typing import Text, Dict, Any
from pathlib import Path
from ai_flow.plugin_interface.blob_manager_interface import BlobManager
from ai_flow.util.file_util.zip_file_util import make_dir_zipfile


class LocalBlobManager(BlobManager):
    """
    LocalBlobManager is an implementation of BlobManager based on the local file system.
    LocalBlobManager contains 2 configuration items:
    1. local_repository: It represents the root path of the downloaded project package.
    2. remote_repository: It represents the root path of the upload project package.
    """
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._local_repo = config.get('local_repository', None)
        self._remote_repo = config.get('remote_repository', None)

    def upload_project(self, workflow_snapshot_id: Text, project_path: Text) -> Text:
        """
        Upload project package.
        If configuration item remote_repository is None, just return the project_path.
        Otherwise package the project and copy it to remote_repository directory.
        :param workflow_snapshot_id: It is the unique identifier for each workflow generation.
        :param project_path: The path of the project.
        """
        if self._remote_repo is not None:
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_file_name = 'workflow_{}_project.zip'.format(workflow_snapshot_id)
                upload_file_path = Path('{}/{}'.format(self._remote_repo, zip_file_name))
                if os.path.exists(upload_file_path):
                    os.remove(upload_file_path)
                temp_dir_path = Path(temp_dir)
                zip_file_path = temp_dir_path / zip_file_name
                make_dir_zipfile(project_path, zip_file_path)
                os.rename(zip_file_path, upload_file_path)
                return str(upload_file_path)
        else:
            return project_path

    def download_project(self, workflow_snapshot_id, remote_path: Text, local_path: Text = None) -> Text:
        """
        Download project package.
        If configuration item local_repository is None, just return the project_path.
        Otherwise unzip the project package to local directory.
        :param workflow_snapshot_id: It is the unique identifier for each workflow generation.
        :param remote_path: The uri of the project package.
        :param local_path: The root path of the downloaded files.
        """
        if local_path is not None or self._local_repo is not None:
            repo_path = local_path if local_path is not None else self._local_repo
            local_zip_file_name = 'workflow_{}_project'.format(workflow_snapshot_id)
            extract_path = str(Path(repo_path) / local_zip_file_name)
            with zipfile.ZipFile(remote_path, 'r') as zip_ref:
                top_dir = os.path.split(zip_ref.namelist()[0])[0]
                downloaded_local_path = Path(extract_path) / top_dir
                if os.path.exists(str(downloaded_local_path)):
                    for root, dirs, files in os.walk(str(downloaded_local_path), topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in dirs:
                            os.rmdir(os.path.join(root, name))
                zip_ref.extractall(extract_path)
            return str(downloaded_local_path)
        else:
            return remote_path

    def cleanup_project(self, workflow_snapshot_id, remote_path: Text):
        pass

