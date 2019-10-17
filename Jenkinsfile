@Library("magic_pipe_jenkins_shared_library") _

properties([
        buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '30')),
        disableConcurrentBuilds(),
        disableResume(),
        parameters([booleanParam(name: 'INITIAL_BUILD', defaultValue: false, description: 'Is this the initial build of this project?')])
])

ansiColor('xterm') {

    node('jenkins-deployer') {

        def scmVars = []
        String acrLoginToken = ""
        String registry = "${env.CONTAINER_REGISTRY}.azurecr.io"

        stage("Git") {
            scmVars = checkout scm
        }

        stage("Authenticate") {
            acrLoginToken = authWithPodIdentity([registry: env.CONTAINER_REGISTRY, environment: env.ENVIRONMENT,
                                                 tag: env.TAG, project: env.PROJECT, location: env.location])
        }

        stage("Gradle Build") {
            if(scmVars.GIT_BRANCH == "master") {
                sh "./gradlew createRelease -Prelease.disableChecks"
            }
            sh "./gradlew assemble"
        }

        String integrationImage = "$registry/aqueduct-pipe:integration-${scmVars.GIT_COMMIT}"
        String ppeImage = "$registry/aqueduct-pipe:ppe-${scmVars.GIT_COMMIT}"
        String liveImage = "$registry/aqueduct-pipe:live-${scmVars.GIT_COMMIT}"
        String latestImage = "$registry/aqueduct-pipe:latest"

        stage('Run Tests') {
            parallel(
                spotbugs: {
                    stage('Spot Bugs') {
                        sh "./gradlew spotbugsMain"
                        def spotbugs = scanForIssues tool: spotBugs(pattern: '**/spotbugs/main.xml')
                        publishIssues issues: [spotbugs]
                    }
                },
                pmd: {
                    stage('Pmd Analysis') {
                        sh "./gradlew pmdMain"
                        def pmd = scanForIssues tool: pmdParser(pattern: '**/pmd/main.xml')
                        publishIssues issues: [pmd]
                    }
                },
                owasp: {
                    stage('OWASP Scan') {
                        sh "./gradlew dependencyCheckAggregate"
                        publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: 'build/reports', reportFiles: 'dependency-check-report.html', reportName: 'Dependency Check', reportTitles: 'Dependency Check Report'])
                    }
                },
                unitTests: {
                    stage('Unit Test') {
                        try {
                            sh "./gradlew test"
                        } catch(err) {
                            junit '**/build/test-results/test/*.xml'
                            throw err
                        }
                    }
                },
                integrationTests: {
                    stage('Integration Test') {
                        try {
                            sh "./gradlew integration"
                        } catch(err) {
                            junit '**/build/test-results/test/*.xml'
                            throw err
                        }
                    }
                }
            )

            stage('Publish Test Report') {
                junit '**/build/test-results/test/*.xml'
            }

            parallel(
                publishTestCoverage: {
                    stage('Publish Coverage Report') {
                        jacoco buildOverBuild: true, changeBuildStatus: true, deltaBranchCoverage: '50', deltaClassCoverage: '50', deltaComplexityCoverage: '50', deltaInstructionCoverage: '50', deltaLineCoverage: '50', deltaMethodCoverage: '50', exclusionPattern: '**/*Spec.class'
                        echo "RESULT: ${currentBuild.result}"
                    }
                },
                dockerBuildAndScan: {
                    stage('Docker build and Scan') {
                        container('docker') {
                            sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"
                            dir("pipe-http-server-cloud") {
                                sh "docker build -t ${integrationImage} ."
                            }

                            sh "docker run --rm  -v /var/run/docker.sock:/var/run/docker.sock -v $HOME/.cache/trivy:/root/.cache/trivy knqyf263/trivy:0.1.2 --quiet --ignore-unfixed $integrationImage"

                            sh "docker push ${integrationImage}"

                            if (params.INITIAL_BUILD) {
                                sh "docker tag ${integrationImage} ${latestImage}"
                                sh "docker push ${latestImage}"
                            }
                        }
                    }
                }
            )





            stage ('Isolated System test') {
                isolatedSystemTest(MP_AQUEDUCT_PIPE_IMAGE_VERSION: "integration-${scmVars.GIT_COMMIT.toString()}")
            }
        }

        if (scmVars.GIT_BRANCH == "master") {

            container('docker') {
                sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"

                stage('Push Aqueduct Pipe To PPE') {
                    sh "docker tag ${integrationImage} ${ppeImage}"
                    sh "docker push ${ppeImage}"
                }
            }

            def version = readFile(file:"VERSION.txt")

            stage('Version Test') {
                versionTest("https://api-ppe.tesco.com/messaging/v1/pipe/_status", version)
            }

            container('docker') {
                sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"

                stage('Tag Latest Image') {
                    sh "docker tag ${ppeImage} ${latestImage}"
                    sh "docker push ${latestImage}"
                }
            }

            stage("Release") {
                sshagent(credentials: ['public_github_key']) {
                    sh "#!/bin/sh -e\n./gradlew release -Prelease.disableChecks -Prelease.pushTagsOnly"
                }
            }

            stage("Publish") {
                withCredentials([usernamePassword(credentialsId: 'nexus', passwordVariable: 'NEXUS_PASSWORD', usernameVariable: 'NEXUS_USERNAME')]) {
                    sh "#!/bin/sh -e\n./gradlew publish -PmavenUser=${NEXUS_USERNAME} -PmavenPassword=${NEXUS_PASSWORD}"
                }
            }

            stage('Push Aqueduct Pipe to Live') {
                container('docker') {
                    sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"
                    sh "docker tag ${ppeImage} ${liveImage}"
                    sh "docker push ${liveImage}"
                }
            }

            stage('Version Test Live') {
                versionTest("https://api.tesco.com/messaging/v1/pipe/_status", version)
            }

        } else {
            stage("GitHub Status") {
                publicGithubStatusUpdate("aqueduct-core", "success", scmVars.GIT_COMMIT)
            }
        }
    }
}
