# Glue-to-RDS VPC Connection: Root Causes and Fixes

## Summary

Connecting an AWS Glue ETL job to an RDS SQL Server instance inside a VPC required more infrastructure than a typical "AWS-to-AWS" connection might suggest, because Glue's job network interfaces (ENIs) are private-IP-only by design (confirmed in AWS's own Glue documentation), regardless of how the surrounding subnet is configured. This meant every AWS service the connection needed to reach outside the RDS instance itself — identity, credentials, storage, logging — required an explicit private network path. Four distinct issues were diagnosed and fixed in sequence; all four were genuinely necessary, not incidental detours.

## Issue 1: Missing IAM permission (`ec2:DescribeVpcs`)

**Symptom:** "Unable to access VPC provided in the connection, Please check the permissions on the IAM role"

**Root cause:** The AWS-managed policy `AWSGlueServiceRole` — the standard baseline policy AWS documents for Glue jobs needing VPC access — does not include `ec2:DescribeVpcs`, despite AWS's own Glue VPC-connection documentation listing it as required. Confirmed directly by inspecting the policy's JSON in the IAM console and by checking the specific action, which showed "No access."

**Fix:** Added a small inline policy to the extract role granting `ec2:DescribeVpcs` (Resource: `*`, matching AWS's own sample policy for this exact scenario).

**Necessary?** Yes. Verified as a genuine gap in AWS's managed policy relative to what Glue's own VPC validation checks for, not a misconfiguration on our part.

## Issue 2: Glue's ENIs can't reach STS

**Symptom:** "Failed to assume the customers role. Verify that your VPC has access to STS."

**Root cause:** Per AWS's Glue documentation: "Each elastic network interface is assigned a private IP address from the IP address range within the subnet you specified. No public IP addresses are assigned." This is unconditional — it applies regardless of whether the subnet itself has a route to an Internet Gateway or auto-assigns public IPs (both of which were already correctly configured on this subnet). Without a public IP, Glue's ENI has no path to the public internet, and therefore no path to AWS's public STS endpoint, which Glue needs to reach in order to assume the IAM role.

**Fix:** Created a VPC interface endpoint for STS (`com.amazonaws.us-east-1.sts`) in the same subnet, with private DNS names enabled, giving the ENI a private path to STS that doesn't depend on internet access.

**Necessary?** Yes. This is the mechanism, not a workaround — any Glue-in-VPC job assuming a role needs this exact fix, and it's a documented AWS pattern (AWS PrivateLink), not something specific to a misconfiguration here.

## Issue 3: Glue's ENIs can't reach Secrets Manager

**Symptom:** "Unable to connect to secrets manager. Please check that your VPC configuration can connect to secrets manager."

**Root cause:** Identical to Issue 2, one dependency later — the connection also needed to retrieve the RDS credentials from Secrets Manager, a separate AWS service with the same private-IP-only reachability problem.

**Fix:** Created a second VPC interface endpoint, this time for Secrets Manager (`com.amazonaws.us-east-1.secretsmanager`), same subnet and configuration pattern as the STS endpoint.

**Necessary?** Yes, same reasoning as Issue 2.

**In hindsight:** once the STS pattern was understood, the Secrets Manager endpoint (and the S3 and CloudWatch Logs endpoints added alongside it) should have been provisioned together rather than discovered one at a time through sequential errors. This was flagged directly during troubleshooting and the remaining endpoints were built proactively rather than waiting for each to fail individually.

## Issue 4: Security group didn't allow Spark's driver/executor traffic pattern

**Symptom:** "Failed to initialize pool: ... Connect timed out" — this occurred *after* all four VPC endpoints (STS, Secrets Manager, S3 gateway, CloudWatch Logs) were in place and confirmed Available, and after confirming RDS itself was reachable via SSMS, ruling out RDS configuration, subnet placement, Network ACLs, and DB status as causes.

**Root cause:** Per AWS's Glue connection troubleshooting documentation: "Apache Spark requires bidirectional connectivity among driver and executor nodes. One of the security groups needs to allow ingress rules on all TCP ports." The security group's self-referencing rules up to this point had been scoped narrowly — port 1433 for RDS, port 443 for the interface endpoints — which covered the specific services being reached but not Spark's own internal driver-to-executor coordination traffic, which uses arbitrary ports.

**Fix:** Added a self-referencing inbound rule for **All TCP** (ports 0–65535), source = the security group itself, superseding the need for the narrower port-specific self-referencing rules (which were left in place, redundant but harmless).

**Necessary?** Yes, and this was the final blocker — the connection succeeded immediately after this fix (following a fresh connection object recreation, since Glue connection objects do not appear to re-validate automatically after the underlying network environment changes).

## What was NOT necessary / not built

- **NAT Gateway** — seriously considered as an alternative to endpoint-by-endpoint troubleshooting, but ultimately not built. Both parties in the review (Claude and a second-opinion review) converged on VPC interface/gateway endpoints being the more cost-appropriate and architecturally sound choice for this project's scale, and that judgment held — the endpoint approach did fully resolve the issue without needing NAT.
- **S3 interface endpoint** — correctly avoided in favor of the S3 **gateway** endpoint, which is free (interface endpoints cost ~$0.01/hour each; the gateway endpoint has no hourly charge and works via a route table entry instead of an ENI).
- **KMS VPC endpoint** — raised as a "possibly required" item in the external review but never actually needed, since this project uses the AWS-managed default encryption keys (`aws/rds`, `aws/s3`) rather than customer-managed KMS keys, which don't require this same connectivity path.

## Net assessment

All four fixes were genuine, necessary corrections to real gaps — not speculative or wasted work. The inefficiency in the process was **sequencing, not substance**: each issue was discovered by hitting it, rather than the full dependency list (IAM `DescribeVpcs` gap, STS, Secrets Manager, S3, CloudWatch Logs, and the all-TCP self-referencing SG rule) being known and provisioned upfront. That sequencing cost time but not money or wrong architecture — nothing built along the way needs to be undone, and nothing unnecessary was left running.

## Cost impact

- STS interface endpoint: ~$7.30/month
- Secrets Manager interface endpoint: ~$7.30/month
- CloudWatch Logs interface endpoint: ~$7.30/month
- S3 gateway endpoint: $0/month
- **Total added recurring cost: ~$22/month** while these endpoints remain provisioned (delete after project completion, same as other resources, to stop billing).
