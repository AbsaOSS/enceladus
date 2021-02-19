# Enceladus Release Scripts

## release_prepare

`release_prepare.sh` / `release_prepare.cmd`

* Removes the `-SNAPSHOT` version suffix in all submodules in the current _develop_ branch and merges it into the _master_ branch.
* In the _develop_ branch it increases the minor version of all the modules of the project.
* It commits and pushes the changes.

**NB! This script is not idempotent. Don't run it repeatedly on the same version.**

## release_deploy

`release_deploy.sh` / `release_deploy.cmd`

* Uploads the current _master_ branch to Maven Central via Sonatype's Nexus Repository Manager.
* Application implementing OpenPGP is required for proper work. 
