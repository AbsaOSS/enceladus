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

// Directly related to Enceladus issue #1347
// This issue existed since v 1.2.X and was fixed after version 2.5.0 was released.
// This script attempts to find corresponding documents in schema_v1 and attachment_v1 collections by
// s.name == a.refName && "datetime from _id being about the same" (about: abs(ts1-ts2) <= epsilon)
var epsilon = 200; // in millis,

db.getCollection('attachment_v1').aggregate([
  {
    $lookup:
      {
        from: "schema_v1",
        let: { rname: "$refName", rid: "$_id", upperBound: {$add: [{"$toDate":"$_id"}, epsilon/2]}, lowerBound: {$subtract: [{"$toDate":"$_id"}, epsilon/2]} },
        pipeline: [
          { $match:
              { $expr:
                  { $and:
                      [
                        { $eq: [ "$name",  "$$rname" ] },
                        { $lte: [{"$toDate":"$_id"}, "$$upperBound"  ] },
                        { $gte: [{"$toDate":"$_id"}, "$$lowerBound"  ] }

                      ]
                  }
              }
          }
        ],
        as: "schemaInfo"
      }
  },
  { $project : { refName: 1, refCollection : 1, refVersion: 1, "schemaInfo.name": 1, "schemaInfo.version": 1, "sc	hemaInfo._id" : 1} }, // root _id is included by default
  { $out : "remapping_evidence1347"} // saves data as collection - relation with issue #1347
]);

var bulkOps = db.remapping_evidence1347.find().map(function (doc) {

  if(doc.schemaInfo.length != 1) {
    print(`Match for ${doc._id} inconclusive (found ${doc.schemaInfo.length} matches), omitted from remapping!`);
    return;
  }

  var schemaData = doc.schemaInfo[0];
  var schemaVersion = schemaData.version;
  var attachmentVersion = doc.refVersion;

  //debug:
  //print(`Match for ${doc.refName}: schema version ${schemaVersion}, attachment version ${attachmentVersion}`)

  if (schemaVersion < attachmentVersion) { // attachment v can be lower if schema was edited (without new attachment being added)

    print(`Attachment ${doc._id} for ${doc.refName}: schema v. ${schemaVersion}, attachment v. ${attachmentVersion} -> ${attachmentVersion-1}`);
    // remap attachment version
    return {
      "updateMany": {
        "filter": { "_id": doc._id } ,
        "update": { "$inc": { "refVersion": NumberInt(-1) } }
      }
    };
  }

});

// bulkOps contain undefined for ops that did not satisfy the "schemaVersion < attachmentVersion" condition
var bulkOpsCleaned = bulkOps.filter(function(d) { return d != undefined})

db.getCollection('attachment_v1').bulkWrite(bulkOpsCleaned);
db.remapping_evidence1347.drop(); // evidence collection cleanup - comment if you want to keep the record of what was done.
