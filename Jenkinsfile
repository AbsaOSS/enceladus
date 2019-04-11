def enceladusSlaveLabel = getEnceladusSlaveLabel()
def toolVersionJava = getToolVersionJava()
def toolVersionMaven = getToolVersionMaven()
def toolVersionGit = getToolVersionGit()
def mavenSettingsId = getMavenSettingsId()
def mavenAdditionalSettingsBuild = getMavenAdditionalSettingsBuild()
def mavenAdditionalSettingsDeploy = getMavenAdditionalSettingsDeploy()
def artifactoryURL = getArtifactoryUrl()


pipeline {
    agent {
        label "${enceladusSlaveLabel}"
    }
    tools {
        jdk "${toolVersionJava}"
        maven "${toolVersionMaven}"
        git "${toolVersionGit}"
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
        timestamps()
    }
    stages {
        stage ('Build') {
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML ${mavenAdditionalSettingsBuild} clean package"
                }
            }
        }
        stage ('Deploy') {
            when {
                expression { 
                    env.BRANCH_NAME == 'develop' || env.BRANCH_NAME == 'master' 
                }
            }
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn versions:set -DnewVersion=${env.GIT_COMMIT}-SNAPSHOT"
                    sh "mvn -s $MAVEN_SETTINGS_XML -DaltDeploymentRepository=${artifactoryURL} ${mavenAdditionalSettingsDeploy} deploy"
                }
            }
         }
         stage ('Deploy to Test Server') {
            when {
                expression { 
                    env.BRANCH_NAME == 'develop' || env.BRANCH_NAME == 'master' 
                }
            }
            steps {
                callSSHPublish()
            }
         }
    }
}
