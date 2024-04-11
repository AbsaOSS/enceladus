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

// Removes trailing slashes from 's3a://.../' paths (ECS-mapped paths and derived further from there)

function stripTrailingSlashOps(collectionName, fieldToStrip) {
  print(`PrepOps: Stripping trailing / from field ${fieldToStrip} collection in ${collectionName}`);
  var count = 0;
  var ops = db[collectionName].find(
    {
      "$and": [
        {[fieldToStrip]: {$regex: "s3a://.*/$"}}
      ]
    }
  ).map(function (doc) {
    var origPath = doc[fieldToStrip];
    var updatedPath = origPath.replace(/\/$/, "");
    print(`${doc.name} v${doc.version} - updating field ${fieldToStrip}: ${origPath} -> ${updatedPath}`);
    count++;

    return {
      "updateOne": {
        "filter": {"_id": doc._id},
        "update": { "$set": { [fieldToStrip]: updatedPath}}
      }
    };
  });

  print(`${count} documents will be adjusted.`);
  print(``);

  return ops;
}

var ops_d1 = stripTrailingSlashOps("dataset_v1", "hdfsPath");
db.getCollection('dataset_v1').bulkWrite(ops_d1);

var ops_d2 = stripTrailingSlashOps("dataset_v1", "hdfsPublishPath");
db.getCollection('dataset_v1').bulkWrite(ops_d2);

var ops_mt1 = stripTrailingSlashOps("mapping_table_v1", "hdfsPath");
db.getCollection('mapping_table_v1').bulkWrite(ops_mt1);

