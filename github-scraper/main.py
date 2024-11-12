import requests
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Optional
import logging
from urllib.parse import urlparse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class GitHubCommitService:
    def __init__(self, token: str):
        """
        Initialize the GitHub commit service.
        
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
        
    def _setup_session(self) -> requests.Session:
        """
        Set up a requests session with retry strategy and rate limit handling.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=5,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4, 8, 16 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.headers)
        return session
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger("GitHubCommitService")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _handle_rate_limit(self, response: requests.Response):
        """
        Handle GitHub API rate limiting.
        
        Args:
            response (requests.Response): Response from GitHub API
        """
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
            self.logger.info(f"Repository visibility: {'private' if repo_data['private'] else 'public'}")
            return True
        else:
            self.logger.error(f"Failed to access repository: {response.status_code} - {response.text}")
            return False

    def get_commits(self, owner: str, repo: str, since: Optional[str] = None, until: Optional[str] = None) -> List[Dict]:
        """
        Fetch all commits from a specific repository with rate limit handling.
        """
        if not self._check_repo_access(owner, repo):
            raise Exception("Repository not accessible. Please check your permissions and token.")
            
        commits = []
        page = 1
        per_page = 100
        
        params = {
            "per_page": per_page,
            "page": page
        }
        
        if since:
            params["since"] = since
        if until:
            params["until"] = until
            
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/commits"
            
            self.logger.info(f"Fetching page {page} of commits...")
            try:
                response = self.session.get(url, params=params)
                self._handle_rate_limit(response)
                response.raise_for_status()
                
                page_commits = response.json()
                self.logger.debug(f"Retrieved {len(page_commits)} commits from page {page}")
                
                if not page_commits:
                    break
                    
                commits.extend(page_commits)
                page += 1
                params["page"] = page
                
                # Add a small delay between requests to be nice to GitHub's API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching commits: {str(e)}")
                raise Exception(f"GitHub API error: {str(e)}")
        
        if not commits:
            self.logger.warning("No commits found in the repository")
            raise Exception("No commits found in the repository. Please check if the repository is empty or if the date range is correct.")
            
        self.logger.info(f"Successfully fetched {len(commits)} commits")
        return commits
    
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
            
        if not processed_commits:
            raise Exception("No commits could be processed successfully")
            
        df = pd.DataFrame(processed_commits)
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date', ascending=False)
    
    def save_commits_to_csv(self, owner: str, repo: str, output_path: str = None,
                          since: str = None, until: str = None) -> str:
        """
        Get commits from a repository and save them to a CSV file.
        """
        commits = self.get_commits(owner, repo, since, until)
        df = self.process_commits(commits)
        
        if output_path is None:
            output_path = f"{owner}_{repo}_commits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        df.to_csv(output_path, index=False)
        self.logger.info(f"Commits saved to: {output_path}")
        return output_path

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
        # Initialize the service with token
        service = GitHubCommitService(token)
        
        # Example repository (replace with your repository details)
        owner = ""
        repo = ""
        
        # Optional date range for commits
        since_date = None  # e.g., "2024-01-01T00:00:00Z"
        until_date = None  # e.g., "2024-12-31T23:59:59Z"
        
        output_file = service.save_commits_to_csv(
            owner,
            repo,
            since=since_date,
            until=until_date
        )
        print(f"Commits saved to: {output_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")