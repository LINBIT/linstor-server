node {
    def GRADLE_HOME = tool name: 'gradle', type: 'hudson.plugins.gradle.GradleInstallation'

    stage('Checkout') {
        checkout scm
        sh 'git submodule update --init'
    }

    stage('Assemble') {
        sh "${GRADLE_HOME}/bin/gradle clean getProtoc assemble"
    }

    stage('Check') {
        // Continue on non-zero exit code from gradle
        sh "${GRADLE_HOME}/bin/gradle check || true"
    }

    stage('JUnit Report') {
        junit 'build/test-results/test/*.xml'
    }
}
