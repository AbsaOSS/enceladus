def nexusRepoDetails = getNexusRepoDetails()
def mavenSettingsId = getMavenSettingsId()
def mavenAdditionalSettings = getMavenAdditionalSettings()
def enceladusSlaveLabel = getEnceladusSlaveLabel()


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
        stage ('Deploy') {
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML -DaltDeploymentRepository=${nexusRepoDetails} deploy:deploy"
                }
            }
        }
    }
}
