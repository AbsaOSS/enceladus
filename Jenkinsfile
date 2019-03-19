def nexusSnapshotRepoDetails = getNexusSnapshotRepoDetails()
def nexusReleaseRepoDetails = getNexusReleaseRepoDetails()
def mavenSettingsId = getMavenSettingsId()
def mavenAdditionalSettings = getMavenAdditionalSettings()
def enceladusSlaveLabel = getEnceladusSlaveLabel()
def deploymentStageRun = 'false'

pipeline {
    agent {
        label "${enceladusSlaveLabel}"
    }
    tools {
        jdk 'openjdk-1.8.0'
        maven 'Maven-3.6.0'
        git 'git-latest'
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
        timestamps()
    }
    stages {
        stage ('Prepare') {
            steps {
                enceladusCreateConfigurationFiles()
            }
        }
        stage ('Build') {
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML clean package ${mavenAdditionalSettings}"
                }
            }
        }
        stage ('Deploy-to-Snapshot') {
            when {
                    branch 'develop'
            }
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML -DaltDeploymentRepository=${nexusSnapshotRepoDetails} -DskipTests -Dmaven.test.skip deploy"
                }
                script {
                    deploymentStageRun = sh(returnStdout: true, script: "echo true")
                }
            }
        }
        stage ('Deploy-to-Release') {
            when {
                    branch 'master'
            }
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML -DaltDeploymentRepository=${nexusReleaseRepoDetails} -DskipTests -Dmaven.test.skip deploy"
                }
                script {
                    deploymentStageRun = sh(returnStdout: true, script: "echo true")
                }
            }
        }
    }
    post {
        success {
            script {
                if (deploymentStageRun != 'true')
                    sh "echo 'Deployment of artifacts to internal repository skipped, branch is not develop/master'"
            }
        }
    }
}
