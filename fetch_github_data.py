import requests
import csv

# Configuration
REPO_OWNER = "username_or_org"  # Replace with the actual owner of the repo
REPO_NAME = "repository_name"  # Replace with the actual repository name
TOKEN = "YOUR_GITHUB_TOKEN"  # Replace with your GitHub token
BASE_URL = "https://api.github.com/repos/"

def fetch_all_commits(repo_owner, repo_name, token):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    })

    all_commits = []
    page = 1
    per_page = 100  # Maximum allowed by GitHub API per request

    while True:
        url = f"{BASE_URL}{repo_owner}/{repo_name}/commits?page={page}&per_page={per_page}"
        response = session.get(url)

        if response.status_code != 200:
            print(f"Error fetching commits: {response.status_code}, {response.json().get('message')}")
            break

        commits = response.json()
        if not commits:
            break

        # Extract commit messages
        for commit in commits:
            commit_message = commit['commit']['message']
            all_commits.append(commit_message)

        page += 1

    return all_commits

def fetch_all_pull_requests(repo_owner, repo_name, token):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    })

    all_prs = []
    page = 1
    per_page = 100  # Maximum allowed by GitHub API per request

    while True:
        url = f"{BASE_URL}{repo_owner}/{repo_name}/pulls?state=all&page={page}&per_page={per_page}"
        response = session.get(url)

        if response.status_code != 200:
            print(f"Error fetching pull requests: {response.status_code}, {response.json().get('message')}")
            break

        prs = response.json()
        if not prs:
            break

        # Extract pull request titles
        for pr in prs:
            pr_title = pr['title']
            all_prs.append(pr_title)

        page += 1

    return all_prs

# Fetch commits and pull requests
commits = fetch_all_commits(REPO_OWNER, REPO_NAME, TOKEN)
pull_requests = fetch_all_pull_requests(REPO_OWNER, REPO_NAME, TOKEN)

# Save to CSV file
output_file = "commits_and_pull_requests.csv"
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Type", "Message"])  # Header

    for message in commits:
        writer.writerow(["Commit", message])
    for pr_title in pull_requests:
        writer.writerow(["Pull Request", pr_title])

print(f"Fetched {len(commits)} commits and {len(pull_requests)} pull requests and saved to '{output_file}'.")
