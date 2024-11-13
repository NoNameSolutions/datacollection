import requests
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Optional, Union
import logging
from urllib.parse import urlparse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class GitHubDataService:
    def __init__(self, token: str):
        """
        Initialize the GitHub data service.
        
        Args:
            token (str): GitHub personal access token for authentication (required)
        """
        if not token:
            raise ValueError("GitHub token is required for accessing repositories")
            
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.session = self._setup_session()
        self.logger = self._setup_logger()
        
    # [Setup methods remain the same]
    def _setup_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.headers)
        return session
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("GitHubDataService")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _handle_rate_limit(self, response: requests.Response):
        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
        
        if remaining <= 1:
            wait_time = reset_time - int(time.time())
            if wait_time > 0:
                self.logger.warning(f"Rate limit nearly exceeded. Waiting {wait_time} seconds...")
                time.sleep(wait_time)

    def _check_repo_access(self, owner: str, repo: str) -> bool:
        """
        Check if the repository is accessible with current credentials.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}"
        response = self.session.get(url)
        
        if response.status_code == 200:
            repo_data = response.json()
            self.logger.info(f"Successfully accessed repository: {repo_data['full_name']}")
            return True
        else:
            self.logger.error(f"Failed to access repository: {response.status_code} - {response.text}")
            return False

    def _paginate_github_data(self, url: str, params: Dict = None) -> List[Dict]:
        """
        Generic method to handle GitHub API pagination.
        """
        all_data = []
        page = 1
        per_page = 100
        
        if params is None:
            params = {}
        
        params.update({"per_page": per_page})
        
        while True:
            params["page"] = page
            try:
                response = self.session.get(url, params=params)
                self._handle_rate_limit(response)
                response.raise_for_status()
                
                page_data = response.json()
                if not page_data:
                    break
                    
                all_data.extend(page_data)
                page += 1
                time.sleep(0.5)  # Be nice to GitHub API
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching data from {url}: {str(e)}")
                raise
                
        return all_data

    # Commit Methods
    def get_commits(self, owner: str, repo: str, since: Optional[str] = None, until: Optional[str] = None) -> List[Dict]:
        """
        Fetch all commits from a specific repository with rate limit handling.
        """
        if not self._check_repo_access(owner, repo):
            raise Exception("Repository not accessible. Please check your permissions and token.")
            
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        params = {}
        
        if since:
            params["since"] = since
        if until:
            params["until"] = until
            
        self.logger.info(f"Fetching commits from {owner}/{repo}")
        return self._paginate_github_data(url, params)

    def get_detailed_commit(self, commit_url: str) -> Dict:
        """
        Get detailed information about a specific commit.
        """
        try:
            response = self.session.get(commit_url)
            self._handle_rate_limit(response)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching detailed commit info: {str(e)}")
            return None

    def process_commits(self, commits: List[Dict]) -> pd.DataFrame:
        """
        Process commits data into a pandas DataFrame.
        """
        if not commits:
            raise Exception("No commits to process")
            
        processed_commits = []
        total_commits = len(commits)
        
        for index, commit in enumerate(commits, 1):
            try:
                if index % 10 == 0:
                    self.logger.info(f"Processing commit {index}/{total_commits}")
                    
                commit_data = {
                    'sha': commit['sha'],
                    'author': commit['commit']['author']['name'],
                    'author_email': commit['commit']['author']['email'],
                    'date': commit['commit']['author']['date'],
                    'message': commit['commit']['message'],
                    'url': commit['html_url'],
                    'changed_files': None,
                    'additions': None,
                    'deletions': None
                }
                
                # Get detailed commit information including stats
                detailed_commit = self.get_detailed_commit(commit['url'])
                if detailed_commit:
                    commit_data.update({
                        'changed_files': detailed_commit.get('stats', {}).get('total', 0),
                        'additions': detailed_commit.get('stats', {}).get('additions', 0),
                        'deletions': detailed_commit.get('stats', {}).get('deletions', 0)
                    })
                    
                processed_commits.append(commit_data)
                
            except KeyError as e:
                self.logger.error(f"Error processing commit {commit.get('sha', 'unknown')}: {str(e)}")
                continue
                
        df = pd.DataFrame(processed_commits)
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date', ascending=False)

    # [Previous Pull Requests and Issues methods remain the same]
    def get_pull_requests(self, owner: str, repo: str, state: str = "all") -> List[Dict]:
        """
        Fetch all pull requests from a repository.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
        params = {"state": state}
        
        self.logger.info(f"Fetching {state} pull requests from {owner}/{repo}")
        return self._paginate_github_data(url, params)

    def process_pull_requests(self, pull_requests: List[Dict]) -> pd.DataFrame:
        """
        Process pull requests data into a pandas DataFrame.
        """
        if not pull_requests:
            raise Exception("No pull requests to process")
            
        processed_prs = []
        
        for pr in pull_requests:
            pr_data = {
                'number': pr['number'],
                'title': pr['title'],
                'state': pr['state'],
                'created_at': pr['created_at'],
                'updated_at': pr['updated_at'],
                'closed_at': pr['closed_at'],
                'merged_at': pr['merged_at'],
                'author': pr['user']['login'],
                'labels': [label['name'] for label in pr['labels']],
                'commits': pr['commits'],
                'additions': pr['additions'],
                'deletions': pr['deletions'],
                'changed_files': pr['changed_files'],
                'url': pr['html_url']
            }
            processed_prs.append(pr_data)
            
        df = pd.DataFrame(processed_prs)
        for date_column in ['created_at', 'updated_at', 'closed_at', 'merged_at']:
            df[date_column] = pd.to_datetime(df[date_column])
            
        return df.sort_values('created_at', ascending=False)

    def get_issues(self, owner: str, repo: str, state: str = "all", labels: str = None) -> List[Dict]:
        """
        Fetch all issues from a repository.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/issues"
        params = {"state": state}
        
        if labels:
            params["labels"] = labels
            
        self.logger.info(f"Fetching {state} issues from {owner}/{repo}")
        return self._paginate_github_data(url, params)

    def process_issues(self, issues: List[Dict]) -> pd.DataFrame:
        """
        Process issues data into a pandas DataFrame.
        """
        if not issues:
            raise Exception("No issues to process")
            
        processed_issues = []
        
        for issue in issues:
            # Skip pull requests (GitHub considers PRs as issues)
            if "pull_request" in issue:
                continue
                
            issue_data = {
                'number': issue['number'],
                'title': issue['title'],
                'state': issue['state'],
                'created_at': issue['created_at'],
                'updated_at': issue['updated_at'],
                'closed_at': issue['closed_at'],
                'author': issue['user']['login'],
                'labels': [label['name'] for label in issue['labels']],
                'comments': issue['comments'],
                'assignees': [assignee['login'] for assignee in issue['assignees']],
                'milestone': issue['milestone']['title'] if issue['milestone'] else None,
                'url': issue['html_url']
            }
            processed_issues.append(issue_data)
            
        df = pd.DataFrame(processed_issues)
        for date_column in ['created_at', 'updated_at', 'closed_at']:
            df[date_column] = pd.to_datetime(df[date_column])
            
        return df.sort_values('created_at', ascending=False)

    def save_data_to_csv(self, df: pd.DataFrame, prefix: str, owner: str, repo: str) -> str:
        """
        Save DataFrame to CSV with appropriate naming.
        """
        output_path = f"{owner}_{repo}_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_path, index=False)
        self.logger.info(f"Data saved to: {output_path}")
        return output_path

    def save_repo_data(self, owner: str, repo: str, data_types: List[str] = None,
                      since: str = None, until: str = None) -> Dict[str, str]:
        """
        Save specified types of repository data to CSV files.
        """
        if data_types is None:
            data_types = ["commits", "pulls", "issues"]
            
        output_files = {}
        
        try:
            if "commits" in data_types:
                commits = self.get_commits(owner, repo, since, until)
                commits_df = self.process_commits(commits)
                output_files["commits"] = self.save_data_to_csv(commits_df, "commits", owner, repo)
                
            if "pulls" in data_types:
                pulls = self.get_pull_requests(owner, repo)
                pulls_df = self.process_pull_requests(pulls)
                output_files["pulls"] = self.save_data_to_csv(pulls_df, "pulls", owner, repo)
                
            if "issues" in data_types:
                issues = self.get_issues(owner, repo)
                issues_df = self.process_issues(issues)
                output_files["issues"] = self.save_data_to_csv(issues_df, "issues", owner, repo)
                
        except Exception as e:
            self.logger.error(f"Error saving repository data: {str(e)}")
            raise
            
        return output_files

# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load token from environment variable
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    
    if not token:
        print("Please set GITHUB_TOKEN environment variable")
        exit(1)
    
    try:
        # Initialize the service
        service = GitHubDataService(token)
        
        # Example repository
        owner = "prometheus-community"
        repo = "helm-charts"
        
        # Fetch all types of data
        output_files = service.save_repo_data(
            owner,
            repo,
            data_types=["commits", "pulls", "issues"],
            since="2024-01-01T00:00:00Z",  # Optional date range
            until=None
        )
        
        print("Data saved to the following files:")
        for data_type, file_path in output_files.items():
            print(f"{data_type}: {file_path}")
            
    except Exception as e:
        print(f"Error: {str(e)}")