# Contributing

## Getting Started

Make sure you have your personal access token setup on github prior to making any pushes/pulls/other modifications as that is the way to authenticate with github now. 

See [Creating a personal access token - GitHub Docs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token), then follow steps in the [creating a personal access token (classic) section](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#creating-a-personal-access-token-classic) specifically. Note that this repo is not using fine-grained personal access tokens.


## Steps to making code contributions

1. Make sure you have the bridge-analytics-template repository cloned to your local computer
2. Make sure you are on the main branch: ```git checkout main```
3. Make sure your local repo has all the latest updates of the remote repo: ```git pull```
4. Create a feature branch off of the main branch. Usually this is named after the Jira ticket that is associated with the work you are doing plus a small description of the work (e.g: etl-273-update-templates): ```git checkout -b <new_branch_name>```
5. After completing work and testing locally, push to your feature branch:
```
git push -u origin <new_branch_name> # for first time pushes to connect your branch to the remote upstream branch
git push # for subsequent pushes
```
6. In Github, create a pull request from your feature branch to point to the main branch of the remote repository.