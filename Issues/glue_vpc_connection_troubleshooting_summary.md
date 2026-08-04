# Scenario Summary: AWS Glue JDBC Connection to RDS SQL Server (VPC Networking Issues)

## What we're trying to accomplish

Building a data pipeline on AWS: an RDS SQL Server Express instance (source data, three tables loaded via SSMS) needs to be read by an AWS Glue ETL job (PySpark) and written to S3. To do this, we're creating a **Glue Connection** — a JDBC connection resource that lets Glue reach the RDS instance, which lives inside a VPC (not publicly exposed to the general internet, though the RDS instance itself has "public access" enabled and a security group scoped to a specific IP for direct SSMS access).

The Glue Connection is configured with:
- JDBC URL to the RDS SQL Server instance
- Credentials pulled from AWS Secrets Manager (not hardcoded)
- An IAM role (`dea-global-partners-glue-extract-role`) with a custom least-privilege policy plus the AWS-managed `AWSGlueServiceRole` policy
- Network settings: the RDS instance's VPC, a specific subnet, and a security group

## Environment details

- Region: us-east-1
- VPC: default VPC (`vpc-07d9c1b401fcef7da`)
- Subnet: `subnet-0fd014cdf9eb9ecdb` (AZ: us-east-1c), auto-assign public IPv4 = enabled, route table has `0.0.0.0/0 → Internet Gateway` confirmed present
- Security group (`global-partners-sql-sg`): inbound rules for MSSQL/1433 from a specific IP (SSMS access), MSSQL/1433 self-referencing (Glue-to-RDS), and later HTTPS/443 self-referencing (for VPC interface endpoints)
- IAM role: trust policy for `glue.amazonaws.com`, custom policy scoped to S3 bucket prefixes + Secrets Manager secret ARN + SSM parameter, plus AWS-managed `AWSGlueServiceRole`

## Sequence of errors encountered, in order

**Error 1:** "Unable to access VPC provided in the connection, Please check the permissions on the IAM role"
- Root cause found: the AWS-managed `AWSGlueServiceRole` policy, confirmed by inspecting its actual JSON, does **not** include the `ec2:DescribeVpcs` action — even though AWS's own documentation for Glue VPC connections lists it as required. Verified via IAM console showing `DescribeVpcs` as "No access" under the attached policy.
- Fix applied: added a small inline policy granting `ec2:DescribeVpcs` (Resource: `*`, matching AWS's own sample policy for this exact scenario) to the Glue role.
- Result: recreating the connection (a stale connection object from before the fix did not self-resolve; had to delete and recreate) resolved this specific error.

**Error 2:** "Failed to assume the customers role. Verify that your VPC has access to STS."
- Interpretation: Glue's job network interfaces (ENIs), which live inside the specified subnet, could not reach AWS's public STS (Security Token Service) endpoint to assume the IAM role, despite: auto-assign public IPv4 being enabled on the subnet, and a confirmed working `0.0.0.0/0 → Internet Gateway` route on the subnet's route table.
- Apparent underlying cause: AWS Glue's ENIs are documented/observed to not reliably obtain outbound internet access even when subnet-level settings suggest they should.
- Fix applied: created a VPC **interface endpoint** for STS (`com.amazonaws.us-east-1.sts`), in the same subnet/AZ, with private DNS names enabled, and added an HTTPS/443 self-referencing rule to the security group (the existing self-referencing rule only covered port 1433/MSSQL).
- Result: resolved the STS-specific error.

**Error 3:** "Unable to connect to secrets manager. Please check that your VPC configuration can connect to secrets manager."
- Same underlying pattern as Error 2, one dependency later — the connection also needs credentials from Secrets Manager, which is a separate AWS service also requiring its own private network path.
- Proposed fix (not yet completed at time of writing): create a second VPC interface endpoint for Secrets Manager (`com.amazonaws.us-east-1.secretsmanager`), same subnet/security group.

## The core problem, stated generally

AWS Glue jobs running inside a VPC do not get default outbound internet access, even with correct subnet/route table/public-IP configuration. Any AWS service outside the VPC that the Glue job needs to reach (STS, Secrets Manager, and — anticipated but not yet confirmed — S3, CloudWatch Logs, or others once the actual ETL job runs, not just the connection test) needs an explicit private network path. This has surfaced as a sequence of individually-discovered errors rather than being knowable as a complete list upfront, because AWS's own documentation does not clearly enumerate the full dependency list for this scenario in one place.

## Two proposed paths forward

### Option A: Continue adding VPC interface endpoints per AWS service
- Add Secrets Manager endpoint now (in progress/next step)
- Add a free S3 **gateway** endpoint (different type than interface endpoints, no hourly cost) before the actual ETL job runs, since it will need to write output to S3
- Possible additional endpoints not yet confirmed necessary: CloudWatch Logs (for job logging)
- Cost: ~$0.01/hour per interface endpoint (~$7.30/month each), so roughly $14-15/month total for STS + Secrets Manager combined; S3 gateway endpoint is free
- Risk: the complete list of required endpoints has not been fully knowable in advance; we've discovered two so far by trial and error, and there is no guarantee a third or fourth won't surface once the actual PySpark job runs (as opposed to just the connection test)

### Option B: Replace all VPC interface endpoints with a single NAT Gateway
- One NAT Gateway (in a public subnet, with an Elastic IP) plus a route table update on the private subnet (`0.0.0.0/0 → NAT Gateway`) gives the subnet general outbound internet access, resolving connectivity to STS, Secrets Manager, S3, CloudWatch Logs, and anything else, through one mechanism rather than one endpoint per service
- Setup time estimate: ~15-20 minutes
- Cost: ~$0.045/hour (~$32/month if left running continuously) plus ~$0.045/GB of data processed — more expensive than Option A's endpoint costs, and notably, this cost accrues continuously whether or not the pipeline is actively being used, unlike most other resources in this build
- Would require deleting the two interface endpoints already created (STS, Secrets Manager) to avoid paying for both approaches simultaneously
- Risk: lower likelihood of further "service X can't be reached" surprises, since it solves general connectivity rather than service-by-service connectivity — but not a 100% guarantee against all possible further issues

## The actual decision to make

Whether to: (a) finish adding the remaining interface endpoints one at a time as they're discovered to be necessary, accepting the risk of further one-off fixes but at lower ongoing cost, or (b) switch to a NAT Gateway now, accepting a higher flat monthly cost in exchange for resolving the general class of "Glue can't reach an AWS service" problems in one step, with an explicit need to remember to delete/stop it when not actively in use given the continuous billing.
