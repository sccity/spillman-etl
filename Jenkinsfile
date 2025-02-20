pipeline {
    agent {
        kubernetes {
            label "${env.JOB_NAME}-${BUILD_NUMBER}"
            yaml '''
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: jnlp
      image: sccity/jenkins-agent-python:0.0.6
      volumeMounts:
        - name: workspace-volume
          mountPath: /home/jenkins/agent

  volumes:
    - name: workspace-volume
      emptyDir: {}

  nodeSelector:
    Name: jenkins-nodes-k8s-prd-aws-us-west2
  tolerations:
    - key: type
      operator: Equal
      value: jenkins
      effect: NoSchedule
            '''
        }
    }

    stages {
        stage('Initialize') {
            steps {
                container('jnlp') {
                    load './jenkins/01_initialize.groovy'
                }
            }
        }

        stage('Configure') {
            steps {
                container('jnlp') {
                    load './jenkins/02_configure.groovy'
                }
            }
        }

        stage('Prepare') {
            steps {
                container('jnlp') {
                    load './jenkins/03_prepare.groovy'
                }
            }
        }

        stage('Test') {
            steps {
                container('jnlp') {
                    load './jenkins/04_test.groovy'
                }
            }
        }
    }

    post {
        success {
            script {
                container('jnlp') {
                    load './jenkins/tag.groovy'
                }
            }
        }
        fixed {
            load './jenkins/fixed.groovy'
        }
        failure {
            load './jenkins/failure.groovy'
        }
    }
}
