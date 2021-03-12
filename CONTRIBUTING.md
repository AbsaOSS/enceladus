                    Copyright 2018 ABSA Group Limited
                  
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
               http://www.apache.org/licenses/LICENSE-2.0
            
     Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
                      limitations under the License.

# How to contribute to Enceladus

## **Did you find a bug?**

* **Ensure the bug has not already been reported** by searching our **[GitHub Issues](https://github.com/AbsaOSS/enceladus/issues)**.
* If you are unable to find an open issue describing the problem, use the **Bug report** template to open a new one. Tag it with the **bug** label.

## **Do you want to request a new feature?**

* **Ensure the feature has not already been requested** by searching our **[GitHub Issues](https://github.com/AbsaOSS/enceladus/issues)**.
* If you are unable to find the feature request, create a new one. Tag it with the **request** label.

## **Do you want to implement a new feature or fix a bug?**

* Check _Issues_ logs for the feature/bug. Check if someone isn't already working on it.
  * If the feature/bug is not yet filed, please write it up first:
    * **"Life, the universe and everything"**
* Fork the repository.
* We follow the [**GitFlow**](https://nvie.com/posts/a-successful-git-branching-model/) branching strategy:
  * Cut your branch from `develop`, add the _GitHub Issue_ in the branch name:
    * **feature/42-life-universe-everything**
    * **bugfix/42-life-universe-everything**
* Code away. Ask away. Work with us.
  * Commit messages should start with a reference to the GitHub Issue and provide a brief description in the imperative mood:
    * **"#42 Answer the ultimate question"**
  * Don't forget to write tests for your work.
* After finishing everything, push to your forked repo and open a Pull Request to our `develop` branch:
  * Pull Request titles should start with the Github Issue number:
    * **"42 Life, the universe and everything"**
  * Ensure the Pull Request description clearly describes the solution.
  * Connect the PR to the _Issue_

## **Do you want to improve the project's documentation?**

The process is similar, just a tad bit simpler, than the feature or bugfix implementation. For documentation changes, an _Issue_ doesn't need to exist. For bigger changes, we still encourage to have one.

To implement documentation changes:
  * Fork the repository
  * Base your changes on the `gh-pages` branch.
      * Name the branch with **ghp/** prefix, if a connected issue exists add its number after the **ghp/** prefix  
        * **ghp/dont-panic**
        * **ghp/42-dont-panic**  
* After finishing, push to your forked repo and open a Pull Request to our `gh-pages` branch.
  * In the Pull Request describe what the changes are about, what was their motivation.
  * Connect the PR to the _Issue_ if it exists

#### Thanks!

The AbsaOSS team
