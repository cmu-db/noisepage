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
    if match and len(match.groups()) == 1:
        return int(match.groups()[0])
    return None


def check_labels_polite(pr_num):
    api_url = r'https://api.github.com/repos/cmu-db/noisepage/issues/{}/labels'
    data = json.loads(urllib.request.urlopen(api_url.format(pr_num)).read().decode('utf-8'))
    labels = [label['name'] for label in data]
    print('API returned: ', labels)
    return 'ready-for-ci' in labels


def check_labels_impolite(pr_num):
    page_url = r'https://github.com/cmu-db/noisepage/pull/{}'
    data = urllib.request.urlopen(page_url.format(pr_num)).read().decode('utf-8')
    data = data[data.find('js-issue-labels'):]
    data = data[:data.find('</div>')]
    print('Web scraping returned: ', data)
    return 'labels/ready-for-ci' in data


if __name__ == '__main__':
    pr_num = get_pr_num()
    # If we can't find a PR number, allow the build to go on.
    if pr_num is None:
        print('PR number: could not parse from absolute path to working directory. Continuing.')
        sys.exit(0)

    # Otherwise, check the labels for the PR.
    is_ready_for_ci = False
    try:
        is_ready_for_ci = check_labels_polite(pr_num)
    except urllib.error.HTTPError:  # Probably rate-limited.
        is_ready_for_ci = check_labels_impolite(pr_num)

    print('URL: https://github.com/cmu-db/noisepage/pull/{}'.format(pr_num))
    if is_ready_for_ci:
        print('PR #{}: found label "ready-for-ci"'.format(pr_num))
        sys.exit(0)
    else:
        print('PR #{}: cannot find label "ready-for-ci"'.format(pr_num))
        sys.exit(1)
