#!groovy
@Library('jenkins-pipeline-shared@master') _
pipeline {
    agent { label 'declarative' }
    options {
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr:'10'))
            timestamps()
            timeout(60)
        }
    stages {
        stage ('Virtualenv setup') {
            steps {
                echo "create virtualenv and install factorytx here"
                // script {
                //          env['SMTOOL_BUILD'] = true
                //          env['SMTOOL_CONFIG_DIR'] = env['WORKSPACE']
                //          sh 'virtualenv factorytx-venv && . factorytx-venv/bin/activate && python $(which pip) install . && python $(which pip) install -r test-requirements.txt'
                //      }
            }
        }
        stage('Test') {
            steps {
                echo 'testing here'
                // sh '. factorytx-venv/bin/activate && python3 -m pytest tests'
            }
        }
        stage("Build") {
            steps {
                echo 'build here'
                // sh 'python3 setup.py sdist'
            }
            post {
                success {
                    echo 'archive here'
                    // archiveArtifacts 'dist/factorytx*.tar.gz'
                }
            }
        }
    }
    post {
        always {
            echo 'clean up'
            // sh 'rm -rf factorytx-venv ** dist'
            sendNotifications currentBuild.result
        }
    }
}
