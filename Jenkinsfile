node {
    stage('Checkout')

    checkout scm

    stage('Build')

    def GRADLE_HOME = tool name: 'gradle', type: 'hudson.plugins.gradle.GradleInstallation'
    sh "${GRADLE_HOME}/bin/gradle clean build"

    stage('JUnit Report')

    junit 'build/test-results/test/*.xml'
}
