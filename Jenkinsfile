pipeline {
     agent {
         docker {
             image 'maven:3-alpine'
         }
     }
     stages {
         stage('Build') {
             steps {
                 sh 'mvn -B -DskipTests clean install'
             }
         }
     }
 }
