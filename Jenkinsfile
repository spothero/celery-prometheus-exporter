#!/bin/groovy

/// Build Pipeline Code
node('k8s-node-10') {
    // We expect the user to supply the following environment variables
    properties([
        parameters([
            string(description: 'The Semver2 Major Version.', name: 'VERSION_MAJOR'),
            string(description: 'The Semver2 Minor Version.', name: 'VERSION_MINOR'),
            string(description: 'The Semver2 Patch Version.', name: 'VERSION_PATCH')
        ])
    ])
    stage('Verify that all necessary environment variables are present') {
        sh """
           echo "######################### Checking Required Environment Variables #############################"
           if [ -z $params.VERSION_MAJOR ] || [ -z $params.VERSION_MINOR ] || [ -z $params.VERSION_PATCH ]; then
             echo "Failed to specify all required environment variables."
             echo "VERSION_MAJOR, VERSION_MINOR, and VERSION_PATCH must be supplied."
             exit 1
           fi
           echo "######################### Completing Environment Check #############################"
        """
    }
    stage('Prep Environment') {
        checkout([$class : 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false,
                extensions: [[$class: 'SubmoduleOption',
                               disableSubmodules: false,
                               parentCredentials: true,
                               recursiveSubmodules: false,
                               reference: '',
                               trackingSubmodules: true]],
                  gitTool: 'Default', submoduleCfg: [],
                  userRemoteConfigs: [[credentialsId: '0a77d46c-d684-4bf0-b432-4e429271c21a', url: 'https://github.com/spothero/celery-prometheus-exporter.git']]])
        env.VERSION = "$params.VERSION_MAJOR.$params.VERSION_MINOR.$params.VERSION_PATCH"
        currentBuild.displayName = env.VERSION
        currentBuild.description = "redis-celery-exporter build $env.Version"
    }
    stage('Docker Image Build') {
        sh """
           echo "######################### Building Docker Container #############################"
           docker build -f Dockerfile-celery3 -t spothero/redis-celery-exporter:$VERSION --build-arg BUILD_VERSION=$VERSION .
           echo "######################### Completing Docker Container Build #############################"
           """
    }
    stage('Docker Image Push') {
        sh """
           echo "######################### Publishing Docker Container #############################"
           docker push spothero/redis-celery-exporter:$VERSION
           echo "######################### End Publishing Docker Container #############################"
           """
    }
}
