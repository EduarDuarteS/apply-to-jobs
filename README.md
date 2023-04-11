# apply-to-jobs
A job may have a limited number of available applications it can  accept and any talent can only apply to 10 jobs per day!

#

# Cloud Formation

1. install AWS CLI
2.  `aws configure`
2.1 put your AWS Access Key ID

command: `aws cloudformation create-stack --stack-name my-stack-name --template-body file://path/to/template.yaml --parameters ParameterKey=my-parameter-name,ParameterValue=my-parameter-value --capabilities CAPABILITY_NAMED_IAM`

example: `aws cloudformation create-stack --stack-name my-job-application-stack --template-body file://C:\Desarrollos\apply-to-jobs\cloudformation.yaml --region us-east-1 --capabilities CAPABILITY_NAMED_IAM`


pip install -r requirements.txt -t ./package


