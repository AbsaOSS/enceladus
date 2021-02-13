# Enceladus Release Scripts

## release_prepare

`release_prepare.sh` / `release_prepare.cmd`

* Merges the current _develop_ branch into the _master_ branch, while removing the `_SNAPSHOT` suffix from the modules' version.
* In the _develop_ branch it increases the minor version of all the modules of the project.
* It commits and pushes the changes.

**NB! This script is not indenpotent. Don't run it repeatedly on the same version.**

## release_deploy

`release_deploy.sh` / `release_deploy.cmd`

* Uploads the current _master_ branch to Maven Central via Sonatype's Nexus Repository Manager.
* Application implementing OpenPGP is required for proper work. 