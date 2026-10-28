"""AWS infrastructure of the Live Sync email-link demo (one CloudFormation stack per name prefix).

  caller dials in -> Connect inbound flow -> InvokeLambdaFunction <prefix>-livesync-send-link
     -> SES email with https://<cloudfront>/#t=<signed token>
  caller opens the link -> CloudFront (S3 origin, OAC) serves the page
     -> page POSTs the token to /api/session (CloudFront -> HTTP API -> <prefix>-livesync-session)
     -> returns contactId + Live Sync keys while the call is active
     -> Touchpoint input "external" binds the page to the phone call (liveSync.contactId)

Stack resources: Secrets Manager secret (CFN-generated signingKey + Live Sync keys written by
deploy_livesync.py), private S3 bucket, CloudFront distribution + origin access control, HTTP API
(POST /api/session only, throttled), two Lambda functions (inline code from lambda/*.py), their roles
and log groups, and the resource policy that lets the Connect instance invoke send-link.

Not in the stack (account-level / instance-level, done by deploy_livesync.py with boto3): SES email
identities, AssociateLambdaFunction on the Connect instance, page upload, Live Sync keys.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import re
from pathlib import Path

import config

logger = logging.getLogger("livesync_infra")

BASE_DIR = Path(__file__).resolve().parent
LAMBDA_DIR = BASE_DIR / "lambda"
TEMPLATE_BODY_MAX = 51_200  # CreateStack TemplateBody limit

# Managed CloudFront policies (ids are global constants).
CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
ALL_VIEWER_EXCEPT_HOST = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
SECURITY_HEADERS = "67f7725c-6f97-4210-82d7-5512b31e9d03"

EMAIL_RE = re.compile(r"^[^@\s<>\"',;]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,63}$")
DEFAULT_LINK_TTL_MINUTES = 30
PLACEHOLDER_SEND_LINK_ARN = "arn:aws:lambda:{region}:{account}:function:{name}"


def stack_name(prefix: str) -> str:
    return f"{prefix}-livesync-page"


def send_link_function_name(prefix: str) -> str:
    return f"{prefix}-livesync-send-link"


def session_function_name(prefix: str) -> str:
    return f"{prefix}-livesync-session"


def placeholder_send_link_arn(region: str, account: str | None, prefix: str) -> str:
    return PLACEHOLDER_SEND_LINK_ARN.format(region=region, account=account or "000000000000",
                                            name=send_link_function_name(prefix))


def _lambda_code(name: str) -> str:
    return (LAMBDA_DIR / name).read_text(encoding="utf-8")


def _log_group(fn_name: str) -> dict:
    return {"Type": "AWS::Logs::LogGroup", "DeletionPolicy": "Delete", "UpdateReplacePolicy": "Delete",
            "Properties": {"LogGroupName": f"/aws/lambda/{fn_name}", "RetentionInDays": 14}}


def _role(fn_name: str, statements: list) -> dict:
    return {
        "Type": "AWS::IAM::Role",
        "Properties": {
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
            "Policies": [{"PolicyName": "least-privilege", "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": {"Fn::Sub": f"arn:${{AWS::Partition}}:logs:${{AWS::Region}}:${{AWS::AccountId}}"
                                         f":log-group:/aws/lambda/{fn_name}:*"}},
                {"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": {"Ref": "Secret"}},
                *statements]}}],
        },
    }


def build_template(*, prefix: str, instance_id: str, language: str, email_from: str, test_email: str,
                   link_ttl_minutes: int, email_subject: str, email_body: str) -> dict:
    """The stack template.  All values are baked in, so a changed value updates the stack."""
    send_fn, session_fn = send_link_function_name(prefix), session_function_name(prefix)
    instance_arn = {"Fn::Sub": f"arn:${{AWS::Partition}}:connect:${{AWS::Region}}:${{AWS::AccountId}}"
                               f":instance/{instance_id}"}
    resources = {
        "Secret": {
            "Type": "AWS::SecretsManager::Secret",
            "Properties": {
                "Description": f"{prefix} Live Sync demo: link signing key and Live Sync keys",
                "GenerateSecretString": {
                    "SecretStringTemplate": json.dumps({"liveSyncApiKey": "", "liveSyncDeploymentKey": ""}),
                    "GenerateStringKey": "signingKey", "PasswordLength": 64, "ExcludePunctuation": True},
            },
        },
        "PageBucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                "PublicAccessBlockConfiguration": {"BlockPublicAcls": True, "BlockPublicPolicy": True,
                                                   "IgnorePublicAcls": True, "RestrictPublicBuckets": True},
                "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
                "BucketEncryption": {"ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            },
        },
        "PageOac": {
            "Type": "AWS::CloudFront::OriginAccessControl",
            "Properties": {"OriginAccessControlConfig": {
                "Name": {"Fn::Sub": f"{prefix}-livesync-${{AWS::Region}}"}, "OriginAccessControlOriginType": "s3",
                "SigningBehavior": "always", "SigningProtocol": "sigv4"}},
        },
        "PageBucketPolicy": {
            "Type": "AWS::S3::BucketPolicy",
            "Properties": {"Bucket": {"Ref": "PageBucket"}, "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Sid": "CloudFrontReadOnly", "Effect": "Allow", "Principal": {"Service": "cloudfront.amazonaws.com"},
                 "Action": "s3:GetObject", "Resource": {"Fn::Sub": "${PageBucket.Arn}/*"},
                 "Condition": {"StringEquals": {"AWS:SourceArn": {
                     "Fn::Sub": "arn:${AWS::Partition}:cloudfront::${AWS::AccountId}:distribution/${Distribution}"}}}},
                {"Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
                 "Resource": [{"Fn::GetAtt": ["PageBucket", "Arn"]}, {"Fn::Sub": "${PageBucket.Arn}/*"}],
                 "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}},
        },
        "Distribution": {
            "Type": "AWS::CloudFront::Distribution",
            "Properties": {"DistributionConfig": {
                "Comment": f"{prefix} Live Sync demo page",
                "Enabled": True, "HttpVersion": "http2and3", "PriceClass": "PriceClass_All",
                "DefaultRootObject": "index.html",
                "Origins": [
                    {"Id": "page", "DomainName": {"Fn::GetAtt": ["PageBucket", "RegionalDomainName"]},
                     "S3OriginConfig": {"OriginAccessIdentity": ""},
                     "OriginAccessControlId": {"Fn::GetAtt": ["PageOac", "Id"]}},
                    {"Id": "api", "DomainName": {"Fn::Sub": "${HttpApi}.execute-api.${AWS::Region}.amazonaws.com"},
                     "CustomOriginConfig": {"OriginProtocolPolicy": "https-only",
                                            "OriginSSLProtocols": ["TLSv1.2"]}},
                ],
                # Caching disabled: a page upload is visible at once, without invalidations.
                "DefaultCacheBehavior": {"TargetOriginId": "page", "ViewerProtocolPolicy": "redirect-to-https",
                                         "AllowedMethods": ["GET", "HEAD"], "Compress": True,
                                         "CachePolicyId": CACHING_DISABLED,
                                         "ResponseHeadersPolicyId": SECURITY_HEADERS},
                "CacheBehaviors": [{"PathPattern": "/api/*", "TargetOriginId": "api",
                                    "ViewerProtocolPolicy": "https-only",
                                    "AllowedMethods": ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"],
                                    "CachePolicyId": CACHING_DISABLED,
                                    "OriginRequestPolicyId": ALL_VIEWER_EXCEPT_HOST,
                                    "ResponseHeadersPolicyId": SECURITY_HEADERS}],
            }},
        },
        "HttpApi": {"Type": "AWS::ApiGatewayV2::Api",
                    "Properties": {"Name": f"{prefix}-livesync-session", "ProtocolType": "HTTP",
                                   "Description": "Live Sync demo session endpoint (behind CloudFront)"}},
        "ApiStage": {"Type": "AWS::ApiGatewayV2::Stage",
                     "Properties": {"ApiId": {"Ref": "HttpApi"}, "StageName": "$default", "AutoDeploy": True,
                                    "DefaultRouteSettings": {"ThrottlingRateLimit": 5, "ThrottlingBurstLimit": 10}}},
        "ApiIntegration": {"Type": "AWS::ApiGatewayV2::Integration",
                           "Properties": {"ApiId": {"Ref": "HttpApi"}, "IntegrationType": "AWS_PROXY",
                                          "IntegrationUri": {"Fn::GetAtt": ["SessionFunction", "Arn"]},
                                          "PayloadFormatVersion": "2.0", "TimeoutInMillis": 10000}},
        "ApiRoute": {"Type": "AWS::ApiGatewayV2::Route",
                     "Properties": {"ApiId": {"Ref": "HttpApi"}, "RouteKey": "POST /api/session",
                                    "Target": {"Fn::Sub": "integrations/${ApiIntegration}"}}},
        "SessionPermission": {"Type": "AWS::Lambda::Permission",
                              "Properties": {"Action": "lambda:InvokeFunction",
                                             "FunctionName": {"Ref": "SessionFunction"},
                                             "Principal": "apigateway.amazonaws.com",
                                             "SourceArn": {"Fn::Sub": "arn:${AWS::Partition}:execute-api:"
                                                                      "${AWS::Region}:${AWS::AccountId}:${HttpApi}"
                                                                      "/*/*/api/session"}}},
        "SessionLogs": _log_group(session_fn),
        "SessionRole": _role(session_fn, [
            {"Effect": "Allow", "Action": "connect:DescribeContact",
             "Resource": {"Fn::Sub": f"arn:${{AWS::Partition}}:connect:${{AWS::Region}}:${{AWS::AccountId}}"
                                     f":instance/{instance_id}/contact/*"}}]),
        "SessionFunction": {
            "Type": "AWS::Lambda::Function", "DependsOn": "SessionLogs",
            "Properties": {"FunctionName": session_fn, "Runtime": "python3.13", "Handler": "index.handler",
                           "Architectures": ["arm64"], "MemorySize": 256, "Timeout": 10,
                           "Role": {"Fn::GetAtt": ["SessionRole", "Arn"]},
                           "Environment": {"Variables": {"SECRET_ARN": {"Ref": "Secret"},
                                                         "CONNECT_INSTANCE_ID": instance_id}},
                           "Code": {"ZipFile": _lambda_code("session.py")}},
        },
        "SendLinkLogs": _log_group(send_fn),
        "SendLinkRole": _role(send_fn, [
            {"Effect": "Allow", "Action": "ses:SendEmail",
             "Resource": {"Fn::Sub": "arn:${AWS::Partition}:ses:${AWS::Region}:${AWS::AccountId}:identity/*"}}]),
        "SendLinkFunction": {
            "Type": "AWS::Lambda::Function", "DependsOn": "SendLinkLogs",
            "Properties": {"FunctionName": send_fn, "Runtime": "python3.13", "Handler": "index.handler",
                           "Architectures": ["arm64"], "MemorySize": 256, "Timeout": 7,
                           "Role": {"Fn::GetAtt": ["SendLinkRole", "Arn"]},
                           "Environment": {"Variables": {
                               "SECRET_ARN": {"Ref": "Secret"},
                               "PAGE_URL": {"Fn::Sub": "https://${Distribution.DomainName}"},
                               "EMAIL_FROM": email_from, "TEST_EMAIL": test_email or "",
                               "LANGUAGE": language, "LINK_TTL_SECONDS": str(link_ttl_minutes * 60),
                               "EMAIL_SUBJECT": email_subject, "EMAIL_BODY": email_body}},
                           "Code": {"ZipFile": _lambda_code("send_link.py")}},
        },
        "SendLinkConnectPermission": {"Type": "AWS::Lambda::Permission",
                                      "Properties": {"Action": "lambda:InvokeFunction",
                                                     "FunctionName": {"Ref": "SendLinkFunction"},
                                                     "Principal": "connect.amazonaws.com",
                                                     "SourceAccount": {"Ref": "AWS::AccountId"},
                                                     "SourceArn": instance_arn}},
    }
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"{prefix} ACXD Live Sync demo: email link page (generated by deploy_livesync.py)",
        "Resources": resources,
        "Outputs": {
            "PageUrl": {"Value": {"Fn::Sub": "https://${Distribution.DomainName}"}},
            "BucketName": {"Value": {"Ref": "PageBucket"}},
            "DistributionId": {"Value": {"Ref": "Distribution"}},
            "SendLinkFunctionArn": {"Value": {"Fn::GetAtt": ["SendLinkFunction", "Arn"]}},
            "SecretArn": {"Value": {"Ref": "Secret"}},
        },
    }


def template_body(template: dict) -> str:
    body = json.dumps(template, ensure_ascii=False, separators=(",", ":"))
    size = len(body.encode("utf-8"))
    if size > TEMPLATE_BODY_MAX:
        raise ValueError(f"stack template is {size} bytes, CreateStack accepts {TEMPLATE_BODY_MAX}")
    return body


# ---------------------------------------------------------------------------
# Stack lifecycle (boto3 cloudformation client passed in)
# ---------------------------------------------------------------------------

FAILED_CREATE_STATES = ("ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "CREATE_FAILED")
BUSY_SUFFIX = "_IN_PROGRESS"


class StackOwnershipError(Exception):
    """A stack with the derived name exists but was not created by this script."""


def describe_stack(cfn, name: str) -> dict | None:
    from botocore.exceptions import ClientError
    try:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
    except ClientError as exc:
        if "does not exist" in str(exc):
            return None
        raise
    return stacks[0] if stacks else None


def stack_owned(stack: dict) -> bool:
    tags = {t.get("Key"): t.get("Value") for t in stack.get("Tags") or []}
    return tags.get(config.CONNECT_OWNER_TAG_KEY) == config.OWNER_TAG


def stack_outputs(stack: dict | None) -> dict:
    return {o["OutputKey"]: o["OutputValue"] for o in (stack or {}).get("Outputs") or []}


def check_stack(cfn, name: str, allow_foreign: bool) -> dict | None:
    """Read-only: the existing stack (owned, or allowed by the override flag) or None."""
    stack = describe_stack(cfn, name)
    if stack is None:
        return None
    if not stack_owned(stack):
        if not allow_foreign:
            raise StackOwnershipError(name)
        logger.warning("stack %s was not created by this script; updating it because of "
                       "--overwrite-untagged-resources", name)
    if stack["StackStatus"].endswith(BUSY_SUFFIX):
        raise RuntimeError(f"stack {name} is busy ({stack['StackStatus']}); wait and rerun")
    return stack


def _failure_events(cfn, name: str, limit: int = 8) -> list:
    try:
        events = cfn.describe_stack_events(StackName=name)["StackEvents"]
    except Exception:  # noqa: BLE001 - diagnostics only
        return []
    return [f"{e.get('LogicalResourceId')}: {e.get('ResourceStatus')} {e.get('ResourceStatusReason')}"
            for e in events if "FAILED" in (e.get("ResourceStatus") or "")][:limit]


def deploy_stack(cfn, name: str, body: str, allow_foreign: bool = False) -> dict:
    """Create or update the stack and wait.  Returns its outputs."""
    from botocore.exceptions import ClientError, WaiterError
    stack = check_stack(cfn, name, allow_foreign)
    if stack is not None and stack["StackStatus"] in FAILED_CREATE_STATES:
        logger.warning("stack %s is %s from a failed create; deleting it before creating again",
                       name, stack["StackStatus"])
        cfn.delete_stack(StackName=name)
        cfn.get_waiter("stack_delete_complete").wait(StackName=name)
        stack = None
    tags = [{"Key": config.CONNECT_OWNER_TAG_KEY, "Value": config.OWNER_TAG}]
    try:
        if stack is None:
            logger.info("creating stack %s (CloudFront takes several minutes)", name)
            cfn.create_stack(StackName=name, TemplateBody=body, Capabilities=["CAPABILITY_IAM"], Tags=tags,
                             OnFailure="ROLLBACK")
            waiter = "stack_create_complete"
        else:
            logger.info("updating stack %s", name)
            try:
                cfn.update_stack(StackName=name, TemplateBody=body, Capabilities=["CAPABILITY_IAM"], Tags=tags)
            except ClientError as exc:
                if "No updates are to be performed" in str(exc):
                    logger.info("stack %s is up to date", name)
                    return stack_outputs(stack)
                raise
            waiter = "stack_update_complete"
        cfn.get_waiter(waiter).wait(StackName=name, WaiterConfig={"Delay": 15, "MaxAttempts": 160})
    except WaiterError as exc:
        for line in _failure_events(cfn, name):
            logger.error("stack event: %s", line)
        raise RuntimeError(f"stack {name} failed: {exc}") from exc
    return stack_outputs(describe_stack(cfn, name))


# ---------------------------------------------------------------------------
# SES, Connect association, page upload, secret
# ---------------------------------------------------------------------------

def ses_account_status(ses) -> dict:
    """Read-only SES GetAccount of the instance region: production access, sending, enforcement."""
    acc = ses.get_account()
    return {"production": bool(acc.get("ProductionAccessEnabled")),
            "sendingEnabled": acc.get("SendingEnabled", True) is not False,
            "enforcement": acc.get("EnforcementStatus") or "HEALTHY"}


def sending_identity(ses, address: str) -> str | None:
    """Which verified SES identity lets `address` send: 'address', 'domain' or None.  Read-only."""
    from botocore.exceptions import ClientError
    domain = address.rsplit("@", 1)[-1]
    for identity, kind in ((address, "address"), (domain, "domain")):
        try:
            got = ses.get_email_identity(EmailIdentity=identity)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "NotFoundException":
                continue
            raise
        if got.get("VerifiedForSendingStatus"):
            return kind
    return None


def list_connect_email_addresses(connect, instance_id: str) -> list:
    """Email addresses of the Connect instance (email channel), via SearchEmailAddresses.  Read-only."""
    found, token = [], None
    while True:
        kwargs = {"InstanceId": instance_id, "MaxResults": 100}
        if token:
            kwargs["NextToken"] = token
        page = connect.search_email_addresses(**kwargs)
        for item in page.get("EmailAddresses") or []:
            if item.get("EmailAddress"):
                found.append({"address": item["EmailAddress"], "displayName": item.get("DisplayName") or "",
                              "id": item.get("EmailAddressId")})
        token = page.get("NextToken")
        if not token:
            return sorted(found, key=lambda a: a["address"].lower())


def find_sender_candidates(connect, ses, instance_id: str) -> dict:
    """Sender addresses that can mail any recipient: Connect instance email addresses whose address or
    domain is verified for sending in SES, in a region where the SES account has production access.

    Returns {"account": ses_account_status, "addresses": [...all instance addresses...],
             "candidates": [...usable ones, each with "verifiedBy"...]}.  Never writes."""
    account = ses_account_status(ses)
    addresses = list_connect_email_addresses(connect, instance_id)
    candidates = []
    if account["production"] and account["sendingEnabled"]:
        for a in addresses:
            kind = sending_identity(ses, a["address"])
            if kind:
                candidates.append({**a, "verifiedBy": kind})
    return {"account": account, "addresses": addresses, "candidates": candidates}


def associate_lambda(connect, instance_id: str, function_arn: str) -> bool:
    """AssociateLambdaFunction unless already associated.  Returns True when it was added."""
    from botocore.exceptions import ClientError
    for page in connect.get_paginator("list_lambda_functions").paginate(InstanceId=instance_id):
        if function_arn in (page.get("LambdaFunctions") or []):
            return False
    try:
        connect.associate_lambda_function(InstanceId=instance_id, FunctionArn=function_arn)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceConflictException":
            return False
        raise
    return True


def upload_page(s3, bucket: str, page_dir: Path) -> list:
    """Upload every file under page_dir (no-cache).  Returns the uploaded keys."""
    keys = []
    for path in sorted(p for p in Path(page_dir).rglob("*") if p.is_file()):
        key = path.relative_to(page_dir).as_posix()
        ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
            ctype += "; charset=utf-8"
        s3.put_object(Bucket=bucket, Key=key, Body=path.read_bytes(), ContentType=ctype,
                      CacheControl="no-cache")
        keys.append(key)
    return keys


def put_live_sync_keys(sm, secret_arn: str, api_key: str, deployment_key: str) -> None:
    """Write the Live Sync keys into the stack secret, keeping signingKey."""
    doc = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
    if not doc.get("signingKey"):
        raise RuntimeError("the stack secret has no signingKey")
    doc.update(liveSyncApiKey=api_key, liveSyncDeploymentKey=deployment_key)
    sm.put_secret_value(SecretId=secret_arn, SecretString=json.dumps(doc))


def live_sync_keys_configured(sm, secret_arn: str) -> bool:
    doc = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
    return bool(doc.get("liveSyncApiKey") and doc.get("liveSyncDeploymentKey"))
