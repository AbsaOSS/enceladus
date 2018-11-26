/*
 * <!--
 *   ~ Copyright 2018 ABSA Group Limited
 *   ~
 *   ~ Licensed under the Apache License, Version 2.0 (the "License");
 *   ~ you may not use this file except in compliance with the License.
 *   ~ You may obtain a copy of the License at
 *   ~     http://www.apache.org/licenses/LICENSE-2.0
 *   ~
 *   ~ Unless required by applicable law or agreed to in writing, software
 *   ~ distributed under the License is distributed on an "AS IS" BASIS,
 *   ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   ~ See the License for the specific language governing permissions and
 *   ~ limitations under the License.
 *   -->
 */

package za.co.absa.models;

import java.time.LocalDateTime;

class BaseModel {

  private String name;
  private String description;
  private int version;
  private String createdBy;
  private String updatedBy;
  private LocalDateTime creationDate;
  private LocalDateTime updateDate;

  public BaseModel(String name, String description, int version, String createdBy,
                   String updatedBy, LocalDateTime creationDate, LocalDateTime updateDate) {
    this.name = name;
    this.description = description;
    this.version = version;
    this.createdBy = createdBy;
    this.updatedBy = updatedBy;
    this.creationDate = creationDate;
    this.updateDate = updateDate;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public int getVersion() {
    return version;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public LocalDateTime getCreationDate() {
    return creationDate;
  }

  public LocalDateTime getUpdateDate() {
    return updateDate;
  }

  public int compareUpdateTimes(BaseModel second) {
    return updateDate.compareTo(second.getUpdateDate());
  }
}
