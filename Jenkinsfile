pipeline {
    agent { label 'docker' }
    options {
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr:'10'))
            timestamps()
            timeout(60)
        }
    stages {
        stage ('Virtualenv setup') {
            steps {
                // sh 'virtualenv factorytx-venv && . factorytx-venv/bin/activate && python $(which pip) install .'
                echo 'setup env here'
            }
        }
        stage('Test') {
            steps {
                // sh '. factorytx-venv/bin/activate && pip install -U pytest && pytest'
                echo 'testing here'
            }
        }
        stage("Build") {
            steps {
                // sh 'python setup.py sdist'
                echo 'build here'
            }
            post {
                success {
                    // archiveArtifacts 'dist/factorytx*.tar.gz'
                    echo 'archive here'
                }
            }
        }
    }
    post {
        always {
            // sh 'rm -rf factorytx-venv ** dist'
            echo 'clean up'
        }
        success {
            slackSend channel: '#coding',
                color: 'good',
                message: "The pipeline ${currentBuild.fullDisplayName} completed successfully."
        }
        failure {
            mail to: 'ops@sightmachine.com',
                subject: "Failed Pipeline: ${currentBuild.fullDisplayName}",
                body: "Something is wrong with ${env.BUILD_URL}"
        }
    }
}
