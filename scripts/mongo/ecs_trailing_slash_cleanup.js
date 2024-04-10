/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Removes trailing slashes from ECS-mapped paths
// It works, but on target it is too slow (e.g. maps only 10 documents at once and then need to rerun)


function stripTrailingSlash(collectionName, fieldToStrip, requiredFieldToExist) {
  print(`Stripping trailing / from field ${fieldToStrip} collection in ${collectionName}`);
  var count = 0;
  db[collectionName].find(
    {
      "$and": [
        {[requiredFieldToExist]: {$exists: true}},
        {[fieldToStrip]: {$regex: "/$"}}
      ]
    }
  ).forEach(function (e, i) {
    var origPath = e[fieldToStrip];
    var updatedPath = origPath.replace(/\/$/, "");
    print(`${e.name} v${e.version} - updating field ${fieldToStrip}: ${origPath} -> ${updatedPath}`);
    e[fieldToStrip] = updatedPath;
    db[collectionName].save(e);
    count++;
  })

  print(`${count} documents adjusted in total.`);
  print(``);
}

stripTrailingSlash("dataset_v1", "hdfsPath", "bakHdfsPath");
stripTrailingSlash("dataset_v1", "hdfsPublishPath", "bakHdfsPublishPath");
stripTrailingSlash("mapping_table_v1", "hdfsPath", "bakHdfsPath");
