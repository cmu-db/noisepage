'''
Check to see if we've tagged the build as ready on GitHub.
Valid tags: ready-for-review, ready-to-merge
If a PR number cannot be detected from the current working directory,
or if the tags are present, then the script exits with error code 0.
Otherwise the script exits with error code 1.
'''
import json
import os
import re
import sys
import urllib.request


def get_pr_num():
    match = re.match(r'.*terrier_PR-([^@/]*).*', os.getcwd())
    if len(match.groups()) == 1:
        return int(match.groups()[0])
    return None


if __name__ == '__main__':
    pr_num = get_pr_num()
    # If we can't find a PR number, allow the build to go on.
    if pr_num is None:
        sys.exit(0)
    # Otherwise, get the labels for the PR.
    api_url = r'https://api.github.com/repos/cmu-db/noisepage/issues/{}/labels'
    req = json.loads(urllib.request.urlopen(api_url.format(pr_num)).read().decode('utf-8'))
    labels = [label['name'] for label in req.json()]

    if 'ready-for-review' in labels or 'ready-to-merge' in labels:
        sys.exit(0)
    else:
        sys.exit(1)
