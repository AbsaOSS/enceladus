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

class ValidationResult {

  constructor(result, isValid, errorMessage) {
    this._result = result;
    this._isValid = isValid;
    this._errorMessage = errorMessage;
  }

  get result() {
    return this._result;
  }

  get isValid() {
    return this._isValid;
  }

  get errorMessage() {
    return this._errorMessage;
  }

}

class ValidResult extends ValidationResult {

  constructor(result) {
    super(result, true);
  }

}

class InvalidResult extends ValidationResult {

  constructor(errorMessage) {
    super(null, false, errorMessage);
  }

}
