# Git

There are lots of guides and documents on the internet, but there are too many and many are confusing. Here is a mini guide to use git with a minimal number of commands and parameters. You won't find any details or explanation of Git's internal mechanisms here.

### Remote Transfer or how to communicate with the world
* Get a fresh repository: git clone `<remote path>`
* Update current repository to latest: git fetch -v 
* Update current repository with commit from a fork: git fetch -v `<remote path>` `<branch>`
* Send your new commit to the remote: git push `<remote>` `<branch>`

### Commit or how to communicate with your local repository
* stage your change with dynamic selection: git add/rm -p `<file>`
* commit your change: git commit
* uncommit previous commit: git reset --soft HEAD~1
* unstage your change: git reset HEAD --
* discard your change **forever** with dynamic selection: git checkout -p -- `<file>`

### Stash or how to save your precious work
Stash is very useful. For example, you will use it before/after (push/pop) merge/rebase action 
* Push pending update on the stack: git stash
* Get back your update: git stash pop
* view content of your stash: git stash show -p `stash@\{0\}`

### Rebase or how to screw the history
**Never** rebase commits that were pushed remotely. Rebase can be used to improve your current patch set, or to fast-forward-merge after a fetch. For better software engineering we **never** directly merge the upstream master when doing local development. When accepting a PR, we expect you to fetch the upstream master to your local repository, and rebase all the changes in your local branch on top of the upstream master.
* The rebase command: git rebase -i `<upstream master branch>`
* Cancel it : git rebase --abort
* Resolve conflict: git mergetool `<file>`
* Continue rebase: git rebase --continue

### Branch or how to separate your work by feature
Please note that master is actually the default branch
* List branches: git branch -v
* Switch to another branch: git checkout `<branch>`
* Creates: git branch `<branch>`
* Delete branches: git branch -d `<branch>`
* Set the base reference of the branch (for rebase): git branch --set-upstream-to=`<remote>` `<branch_name>`

# Git use case example

### Branch management
Let's say you want to rebase your current branch topic-v1 to topic-v2 with new additions. Note: topic-v1 could also be master too.
* Go to current branch: git checkout topic-v1
* Create a new one: git branch topic-v2
* Go into the new branch: git checkout topic-v2
* Set the reference: git branch --set-upstream-to=origin/master topic-v2 
* Rebase: git rebase -i
* ...

### Split commit
* Copy your repository if you're not confident with this kind of operation: cp -a `<repository>` `<repository backup>`
* Do a rebase: git rebase -i
* Use edit on the commit that you want to split
... rebase on-going...
* Uncommit: git reset --soft HEAD~1
* Unstage: git reset HEAD --

At this stage of operation, you get all your changes in the local files, but nothing is ready to be committed.

Repeat the 2 next commands for each new commits that you want to create
* Stage your change with dynamic selection: git add/rm -p `<file>`
* Commit your change: git commit

Once you have finished to split your commit:
* Finish the rebase: git rebase --continue