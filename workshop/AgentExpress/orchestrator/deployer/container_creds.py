"""Print this container's own AWS credentials in `credential_process` format.

The deploy runner's `console` profile (runner.Target.env) points here on the Terraform
path: the AWS SDK for Go does not accept `credential_source = EcsContainer` without a
`role_arn`, but every SDK accepts `credential_process`. CodeBuild, like ECS, serves the
project role's credentials from the container endpoint named by
AWS_CONTAINER_CREDENTIALS_RELATIVE_URI (or _FULL_URI with an authorization token).
Standard library only; the credentials go to stdout for the SDK and nowhere else.
"""

import json
import os
import sys
import urllib.request


def main() -> int:
    relative = os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    url = (f"http://169.254.170.2{relative}" if relative
           else os.environ.get("AWS_CONTAINER_CREDENTIALS_FULL_URI", ""))
    if not url:
        print("container_creds: no container credentials endpoint in the environment",
              file=sys.stderr)
        return 1
    req = urllib.request.Request(url)  # noqa: S310 - the fixed link-local/container endpoint
    token = os.environ.get("AWS_CONTAINER_AUTHORIZATION_TOKEN")
    if token:
        req.add_header("Authorization", token)
    with urllib.request.urlopen(req, timeout=5) as r:  # noqa: S310 - the fixed container endpoint  # nosec B310
        c = json.load(r)
    print(json.dumps({"Version": 1, "AccessKeyId": c["AccessKeyId"],
                      "SecretAccessKey": c["SecretAccessKey"], "SessionToken": c["Token"],
                      "Expiration": c["Expiration"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
